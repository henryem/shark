package blinkdb

import shark.LogHelper
import shark.execution.SparkTask
import org.apache.hadoop.hive.ql.parse.ParseUtils
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer
import blinkdb.parse.InputExtractionSemanticAnalyzer
import shark.parse.QueryContext
import blinkdb.parse.BlinkDbSemanticAnalyzerFactory
import spark.RDD
import shark.execution.HiveOperator
import shark.execution.SparkWork
import org.apache.hadoop.hive.ql.parse.VariableSubstitution
import org.apache.hadoop.hive.ql.exec.Operator
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.parse.ParseDriver
import scala.collection.JavaConversions._
import shark.execution.serialization.KryoSerializer
import blinkdb.util.HiveUtils
import shark.execution.IntermediateCacheOperator
import akka.dispatch.Future
import blinkdb.util.FutureRddOps._
import akka.dispatch.ExecutionContext

object QueryRunner extends LogHelper {

  def logOperatorTree(sem: BaseSemanticAnalyzer): Unit = {
    if (!log.isDebugEnabled()) {
      return
    }
    val sourceOperators: Seq[shark.execution.Operator[_]] = getSourceOperators(sem)
    def visit(operator: shark.execution.Operator[_]) {
      logDebug(
          "Operator %s, hiveOp %s, objectInspectors %s, children %s".format(
              operator,
              operator.hiveOp.getClass(),
              Option(operator.hiveOp.asInstanceOf[Operator[_]].getInputObjInspectors()).map(inspectors => inspectors.deep.toString()),
              Option(operator.childOperators).map(operators => operators.map(_.getClass()))))
      operator.childOperators.foreach(visit)
    }
    sourceOperators.foreach(visit)
  }
  
  def doSemanticAnalysis(cmd: String, stage: ErrorAnalysisStage, conf: HiveConf, inputRdd: Option[RDD[Any]]): BaseSemanticAnalyzer = {
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
  
  def getIntermediateInputOperators(sem: BaseSemanticAnalyzer): Seq[IntermediateCacheOperator] = {
    //HACK
    Seq(sem.asInstanceOf[InputExtractionSemanticAnalyzer].intermediateInputOperator)
  }
  
  /** 
   * Initialize all operators in the operator tree contained in @sem.  After
   * this, it is okay to call execute() on any operator in this tree.
   */
  def initializeOperatorTree(sem: BaseSemanticAnalyzer): Unit = {
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
  
  /** 
   * Execute the operator tree in @sem, producing an output RDD and an
   * ObjectInspector that can be used to interpret its rows.
   */
  def executeOperatorTree(sem: BaseSemanticAnalyzer): (RDD[Any], StructObjectInspector) = {
    val sinkOperators: Seq[shark.execution.Operator[_]] = getSinkOperators(sem)
    initializeOperatorTree(sem)
    logOperatorTree(sem)
    //TODO: Handle more than 1 sink.
    require(sinkOperators.size == 1, "During bootstrap: Found %d sinks, expected 1.".format(sinkOperators.size))
    val sinkOperator = sinkOperators(0).asInstanceOf[shark.execution.TerminalOperator]
    require(sinkOperator.objectInspector.isInstanceOf[StructObjectInspector], "During bootstrap: Expected output rows to be Structs, but encountered something else.")
    (sinkOperator.execute().asInstanceOf[RDD[Any]], sinkOperator.objectInspector.asInstanceOf[StructObjectInspector])
  }
  
  
  /**
   * Collect outputs from query runs @outputRdds.  Currently, rows are
   * expected to have only numeric primitive fields.
   * 
   * This is just a convenience method for mapping collectSingleQueryOutput()
   * over a sequence of RDDs.
   */
  def collectQueryOutputs(outputRdds: Seq[(RDD[_], StructObjectInspector)])(implicit ec: ExecutionContext): Future[Seq[SingleQueryIterateOutput]] = {
    //TODO: Share ObjectInspectors across RDDs.  Serializing them repeatedly
    // here is wasteful.
    Future.sequence(outputRdds.map({ case (outputRdd, objectInspector) => collectSingleQueryOutput(outputRdd, objectInspector) }))
  }
  
  /** 
   * Collect outputs from a single query run @rdd, using @objectInspector
   * to inspect each row.  Currently, rows are expected to have only numeric
   * primitive fields.
   */
  def collectSingleQueryOutput(
      rdd: RDD[_],
      objectInspector: StructObjectInspector)
      (implicit ec: ExecutionContext):
      Future[SingleQueryIterateOutput] = {
    val objectInspectorSerialized = KryoSerializer.serialize(objectInspector)
    val rawOutputsFuture = rdd
      .map(hiveRow => HiveUtils.toNumericRow(hiveRow, KryoSerializer.deserialize(objectInspectorSerialized)))
      .collectFuture()
    rawOutputsFuture.map(rawOutputs => {
      val numRows = rawOutputs.size
      val numFields = if (numRows > 0) rawOutputs(0).size else 0
      SingleQueryIterateOutput(rawOutputs, numRows, numFields)
    })
  }
}