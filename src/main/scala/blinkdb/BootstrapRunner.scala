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
  def runForResult(cmd: String): Option[String] = {
    val inputRdd: Option[RDD[Any]] = makeInputRdd(cmd)
    inputRdd.map(rdd => doBootstrap(cmd, rdd))
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
  
  private def doBootstrap(cmd: String, inputRdd: RDD[Any]): String = {
    val resampleRdds = ResampleGenerator.generateResamples(inputRdd, BootstrapRunner.NUM_BOOTSTRAP_RESAMPLES)
    val resultRdds = resampleRdds.map({ resampleRdd => 
      //TODO: Reuse semantic analysis across runs.  For now this avoids the
      // hassle of reaching into the graph and replacing the resample RDD,
      // and it also avoids any bugs that might result from executing an
      // operator graph more than once.
      val sem = BootstrapRunner.doSemanticAnalysis(cmd, BootstrapStage.BootstrapExecution, conf, Some(resampleRdd))
      val sinkOperators: Seq[shark.execution.Operator[_]] = BootstrapRunner.getSinkOperators(sem)
      BootstrapRunner.initializeOperatorTree(sem)
      logOperatorTree(sem)
      //TODO: Handle more than 1 sink.
      require(sinkOperators.size == 1, "During bootstrap: Found %d sinks, expected 1.".format(sinkOperators.size))
      val sinkOperator = sinkOperators(0).asInstanceOf[shark.execution.TerminalOperator]
      (sinkOperator.execute(), sinkOperator.objectInspector)
    })
    val sem = BootstrapRunner.doSemanticAnalysis(cmd, BootstrapStage.BootstrapExecution, conf, Some(SharkEnv.sc.parallelize(Seq()))) //HACK: This RDD won't be used, but we need to pass one.
    val bootstrapOutputs = BootstrapRunner.collectBootstrapOutputs(resultRdds, sem)
    BootstrapRunner.computeErrorQuantification(bootstrapOutputs).toString
  }
}

object BootstrapRunner {
  val NUM_BOOTSTRAP_RESAMPLES = 10
  
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
  
  private def collectBootstrapOutputs(outputRdds: Seq[(RDD[_], ObjectInspector)], sem: BaseSemanticAnalyzer): Seq[Double] = {
    //TODO: Share ObjectInspectors across RDDs.  Serializing them repeatedly
    // here is wasteful.
    outputRdds
      .par
      .map({ case (outputRdd, objectInspector) => 
        //NOTE: We have to transform the rows to numbers _before_ collecting
        // them.  Otherwise we will try to collect a bunch of DoubleWritables
        // and fail because Writables are not Java serializable.
        val objectInspectorSerialized = KryoSerializer.serialize(objectInspector)
        val rawOutputs = outputRdd
          .map(hiveRow => toNumericOutput(hiveRow, KryoSerializer.deserialize(objectInspectorSerialized)))
          .collect()
        //TODO: Support multi-row outputs (e.g. group-bys)
        require(rawOutputs.size == 1, "Currently only queries with single-row outputs are supported!")
        rawOutputs(0)
      })
      .seq
  }
  
  private def toNumericOutput(rawOutput: Any, objectInspector: ObjectInspector): Double = {
    //FIXME: This could be cleaned up quite a bit.  Also, we may want
    // to allow more types.
    require(objectInspector.getCategory() == Category.STRUCT)
    val structOi = objectInspector.asInstanceOf[StructObjectInspector]
    val struct = structOi.getStructFieldsDataAsList(rawOutput)
    val firstFieldOi = structOi.getAllStructFieldRefs().get(0).getFieldObjectInspector()
    require(firstFieldOi.getCategory() == Category.PRIMITIVE)
    val primitiveOi = firstFieldOi.asInstanceOf[PrimitiveObjectInspector]
    val primitiveFieldValue = primitiveOi.getPrimitiveJavaObject(struct.get(0))
    primitiveFieldValue match {
      case d: java.lang.Double => d.asInstanceOf[java.lang.Double]
      case f: java.lang.Float => f.asInstanceOf[java.lang.Float].toDouble
      case i: java.lang.Integer => i.asInstanceOf[java.lang.Integer].toDouble
      case l: java.lang.Long => l.asInstanceOf[java.lang.Long].toDouble
      case other => throw new IllegalArgumentException("Unexpected aggregate value type for bootstrap: %s".format(other))
    }
  }
  
  private def computeErrorQuantification(bootstrapOutputs: Seq[Double]): Double = {
    //TODO: Define a BootstrapOutput class instead.
    standardDeviation(bootstrapOutputs)
  }
  
  private def standardDeviation(numbers: Seq[Double]): Double = {
    //TODO: This is inefficient.
    println("Computing standard deviation of %s".format(numbers))
    val count = numbers.size
    val sum = numbers.sum
    val sumSq = numbers.map(number => number*number).sum
    if (count == 0) {
      0.0
    } else {
      math.sqrt((sumSq - sum*sum/count) / count)
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