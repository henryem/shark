package blinkdb
import edu.berkeley.blbspark.StratifiedBlb
import shark.execution.SparkTask
import org.apache.hadoop.hive.ql.parse.ParseUtils
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer
import shark.parse.InputExtractionSemanticAnalyzer
import shark.parse.QueryContext
import shark.parse.BlinkDbSemanticAnalyzerFactory
import spark.RDD
import shark.execution.HiveOperator
import shark.execution.SparkWork
import edu.berkeley.blbspark.WeightedItem
import org.apache.hadoop.hive.ql.parse.VariableSubstitution
import org.apache.hadoop.hive.ql.exec.Operator
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import shark.execution.serialization.KryoSerializer
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import shark.BootstrapStage
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer
import shark.SharkEnv
import org.apache.hadoop.hive.ql.parse.ParseDriver
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import shark.LogHelper

class BootstrapRunner(conf: HiveConf) extends LogHelper {
  /** 
   * @return one ErrorQuantification for each row and field in the output of
   *   @cmd.  For example, if there are two cities, NY and SF, and we have the
   *   following query:
   *     SELECT AVG(salary), AVG(height), AVG(weight) FROM t GROUP BY city;
   *   ...then the bootstrap output might look like:
   *     Seq(Seq(100.0, 2.0, 4.0), Seq(102.0, 2.1, 3.9))
   *   
   *   If no bootstrap is run (e.g. because the query is creating a table or
   *   updating rows, rather than selecting aggregates), None is returned.
   *   
   *   TODO: Wrap the return value into a single object.
   *   TODO: Support columns without error quantifications, e.g. adding city as
   *     one of the selected columns in the above query.
   */
  def runForResult[E <: ErrorQuantification](
      cmd: String,
      errorQuantifier: ErrorQuantifier[E]):
      Option[Seq[Seq[ErrorQuantification]]] = {
    val inputRdd: Option[RDD[Any]] = makeInputRdd(cmd)
    inputRdd.map(rdd => doBootstrap(cmd, rdd, errorQuantifier))
  }
  
  /** 
   * Make an RDD containing the input to @cmd, suitable for insertion in @cmd
   * via an RddScanOperator.
   * 
   * @return None if @cmd is not suitable for extracting input.
   */
  private def makeInputRdd(cmd: String): Option[RDD[Any]] = {
    val sem = BootstrapRunner.doSemanticAnalysis(cmd, BootstrapStage.InputExtraction, conf, None)
    if (!sem.isInstanceOf[InputExtractionSemanticAnalyzer]
        || !sem.asInstanceOf[SemanticAnalyzer].getParseContext().getQB().getIsQuery()) {
      //TODO: With Sameer's SQL parser, this will be unnecessary - we can just
      // check whether the user explicitly asked for an approximation.  For now
      // we execute the bootstrap on anything that looks like a query.
      None
    } else {
      val intermediateInputOperators: Seq[shark.execution.Operator[_]] = BootstrapRunner.getIntermediateInputOperators(sem)
      BootstrapRunner.initializeOperatorTree(sem)
    
      //TODO: Handle more than 1 sink.
      require(intermediateInputOperators.size == 1)
      Some(intermediateInputOperators(0).execute().asInstanceOf[RDD[Any]])
    }
  }
  
  private def doBootstrap[E <: ErrorQuantification](
      cmd: String,
      inputRdd: RDD[Any],
      errorQuantifier: ErrorQuantifier[E]):
      Seq[Seq[E]] = {
    val resampleRdds = ResampleGenerator.generateResamples(inputRdd, BootstrapRunner.NUM_BOOTSTRAP_RESAMPLES)
    val resultRdds = resampleRdds.map({ resampleRdd => 
      //TODO: Reuse semantic analysis across runs.  For now this avoids the
      // hassle of reaching into the graph and replacing the resample RDD,
      // and it also avoids any bugs that might result from executing an
      // operator graph more than once.
      val sem = BootstrapRunner.doSemanticAnalysis(cmd, BootstrapStage.BootstrapExecution, conf, Some(resampleRdd))
      val sinkOperators: Seq[shark.execution.Operator[_]] = BootstrapRunner.getSinkOperators(sem)
      BootstrapRunner.initializeOperatorTree(sem)
      BootstrapRunner.logOperatorTree(sem)
      //TODO: Handle more than 1 sink.
      require(sinkOperators.size == 1, "During bootstrap: Found %d sinks, expected 1.".format(sinkOperators.size))
      val sinkOperator = sinkOperators(0).asInstanceOf[shark.execution.TerminalOperator]
      require(sinkOperator.objectInspector.isInstanceOf[StructObjectInspector], "During bootstrap: Expected output rows to be Structs, but encountered something else.")
      (sinkOperator.execute(), sinkOperator.objectInspector.asInstanceOf[StructObjectInspector])
    })
    val sem = BootstrapRunner.doSemanticAnalysis(cmd, BootstrapStage.BootstrapExecution, conf, Some(SharkEnv.sc.parallelize(Seq()))) //HACK: This RDD won't be used, but we need to pass one.
    val bootstrapOutputs = BootstrapRunner.collectBootstrapOutputs(resultRdds)
    //TODO: Make this pluggable.
    errorQuantifier.computeError(bootstrapOutputs)
  }
}

object BootstrapRunner extends LogHelper {
  val NUM_BOOTSTRAP_RESAMPLES = 10
  
  private def logOperatorTree(sem: BaseSemanticAnalyzer): Unit = {
    if (!log.isDebugEnabled()) {
      return
    }
    val sourceOperators: Seq[shark.execution.Operator[_]] = BootstrapRunner.getSourceOperators(sem)
    def visit(operator: shark.execution.Operator[_]) {
      log.debug(
          "Operator %s, hiveOp %s, objectInspectors %s, children %s".format(
              operator,
              operator.hiveOp.getClass(),
              Option(operator.hiveOp.asInstanceOf[Operator[_]].getInputObjInspectors()).map(inspectors => inspectors.deep.toString()),
              Option(operator.childOperators).map(operators => operators.map(_.getClass()))))
      operator.childOperators.foreach(visit)
    }
    sourceOperators.foreach(visit)
  }
  
  private def doSemanticAnalysis(cmd: String, stage: BootstrapStage, conf: HiveConf, inputRdd: Option[RDD[Any]]): BaseSemanticAnalyzer = {
    val command = new VariableSubstitution().substitute(conf, cmd)
    val context = new QueryContext(conf, false)
    context.setCmd(cmd)
    context.setTryCount(Integer.MAX_VALUE)

    val tree = ParseUtils.findRootNonNullToken((new ParseDriver()).parse(command, context))
    val sem = BlinkDbSemanticAnalyzerFactory.get(conf, tree, stage, inputRdd)
    
    //TODO: Currently I do not include configured SemanticAnalyzer hooks.
    sem.analyze(tree, context)
    sem.validate()
    sem
  }
  
  private def getSourceOperators(sem: BaseSemanticAnalyzer): Seq[shark.execution.Operator[_]] = {
    sem.getRootTasks()
      .map(_.getWork().asInstanceOf[SparkWork].terminalOperator.asInstanceOf[shark.execution.TerminalOperator])
      .flatMap(_.returnTopOperators())
      .distinct
  }
  
  private def getSinkOperators(sem: BaseSemanticAnalyzer): Seq[shark.execution.Operator[_]] = {
    getSourceOperators(sem).flatMap(_.returnTerminalOperators()).distinct
  }
  
  private def getIntermediateInputOperators(sem: BaseSemanticAnalyzer): Seq[shark.execution.Operator[_]] = {
    //HACK
    Seq(sem.asInstanceOf[InputExtractionSemanticAnalyzer].intermediateInputOperator.asInstanceOf[shark.execution.Operator[_ <: HiveOperator]])
  }
  
  // Initialize all operators in the operator tree contained in @sem.  After
  // this, it is okay to call execute() on any operator in this tree.
  private def initializeOperatorTree(sem: BaseSemanticAnalyzer): Unit = {
    val executionTask = sem.getRootTasks().get(0)
    require(executionTask.isInstanceOf[SparkTask])
    val work = executionTask.getWork()
    require(work.isInstanceOf[SparkWork])
    val terminalOp = work.asInstanceOf[SparkWork].terminalOperator
    val tableScanOps = terminalOp.returnTopOperators().asInstanceOf[Seq[shark.execution.TableScanOperator]]
    SparkTask.initializeTableScanTableDesc(tableScanOps, work.asInstanceOf[SparkWork])
    SparkTask.initializeAllHiveOperators(terminalOp)
    terminalOp.initializeMasterOnAll()
  }
  
  // Collect outputs from bootstrap runs @outputRdds.  All of them are
  // collected at once so that we can collect them concurrently, which may be
  // advantageous if individual runs do not use all available cluster
  // resources.
  private def collectBootstrapOutputs(outputRdds: Seq[(RDD[_], StructObjectInspector)]): Seq[BootstrapOutput] = {
    //TODO: Share ObjectInspectors across RDDs.  Serializing them repeatedly
    // here is wasteful.
    outputRdds
      .par
      .map({ case (outputRdd, objectInspector) => collectSingleBootstrapOutput(outputRdd, objectInspector) })
      .seq
  }
  
  // Collect outputs from a single bootstrap run @rdd, using @objectInspector
  // to inspect each row.  Currently, rows are expected to have only numeric
  // primitive fields.
  private def collectSingleBootstrapOutput(rdd: RDD[_], objectInspector: StructObjectInspector): BootstrapOutput = {
    val objectInspectorSerialized = KryoSerializer.serialize(objectInspector)
    val rawOutputs = rdd
      .map(hiveRow => toNumericRow(hiveRow, KryoSerializer.deserialize(objectInspectorSerialized)))
      .collect()
    val numRows = rawOutputs.size
    val numFields = if (numRows > 0) rawOutputs(0).size else 0
    BootstrapOutput(rawOutputs, numRows, numFields)
  }
  
  private def toNumericRow(rawOutput: Any, objectInspector: StructObjectInspector): Seq[Double] = {
    val struct = objectInspector.getStructFieldsDataAsList(rawOutput)
    val structFieldRefs = objectInspector.getAllStructFieldRefs()
    (0 until structFieldRefs.size).map({fieldIdx =>
      val fieldOi = objectInspector.getAllStructFieldRefs().get(fieldIdx).getFieldObjectInspector()
      require(fieldOi.getCategory() == Category.PRIMITIVE)
      val primitiveOi = fieldOi.asInstanceOf[PrimitiveObjectInspector]
      val primitiveFieldValue = primitiveOi.getPrimitiveJavaObject(struct.get(fieldIdx))
      primitiveToDouble(primitiveFieldValue)
    })
  }
  
  private def primitiveToDouble(primitive: Object): Double = {
    //FIXME: May want to allow more types.  Note that Hive's
    // PrimitiveObjectInspectorUtils.getDouble() is not suitable, since it
    // merrily converts Strings and other inappropriate types to doubles.
    primitive match {
      case d: java.lang.Double => d.asInstanceOf[java.lang.Double]
      case f: java.lang.Float => f.asInstanceOf[java.lang.Float].toDouble
      case i: java.lang.Integer => i.asInstanceOf[java.lang.Integer].toDouble
      case l: java.lang.Long => l.asInstanceOf[java.lang.Long].toDouble
      case other => throw new IllegalArgumentException("Unexpected aggregate value type for bootstrap: %s".format(other))
    }
  }
}

object ResampleGenerator {
  def generateResamples[I: ClassManifest](originalRdd: RDD[I], numResamples: Int): Seq[RDD[I]] = {
    val originalTableWithWeights: RDD[WeightedItem[I]] = originalRdd.map(toWeightedRow)
    //TODO: Use BLB instead of bootstrap here.
    val resamples: Seq[RDD[WeightedItem[I]]] = StratifiedBlb.createBootstrapResamples(
        originalTableWithWeights,
        numResamples,
        originalTableWithWeights.partitions.length,
        012 //FIXME: Random seed here.
        )
     resamples.map(_.flatMap(fromWeightedRow))
  }
  
  private def toWeightedRow[I](row: I): WeightedItem[I] = {
    WeightedItem(row, 1.0)
  }
  
  private def fromWeightedRow[I](weightedRow: WeightedItem[I]): Iterator[I] = {
    //FIXME: This is copied from blbspark's WeightedRepeatingIterable.
    require(weightedRow.weight == math.round(weightedRow.weight))
    Iterator.empty.padTo(math.round(weightedRow.weight).asInstanceOf[Int], weightedRow.item)
  }
}

