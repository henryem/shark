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
import org.apache.hadoop.hive.ql.parse.SemanticException
import blinkdb.parse.BootstrapSemanticAnalyzer
import akka.dispatch.Await
import akka.util.Duration
import javax.annotation.Nullable
import shark.execution.serialization.SerializableWritable
import org.apache.hadoop.hive.ql.session.SessionState
import shark.execution.TerminalOperator
import org.apache.hadoop.hive.ql.exec.Task
import shark.execution.RddScanOperator
import java.io.InvalidObjectException

trait QueryExecutionBuilder[B <: QueryExecutionBuilder[B]] extends Serializable {
  def forStage(errorAnalysisStage: ErrorAnalysisStage): B
  def build(): QueryExecution
}

//TODO: Document.
//NOTE: Implementations are generally not serializable.  Use a
// QueryExecutionBuilder to store and move around common data for query
// execution, then build a QueryExecution immediately before you use it.
trait QueryExecution {
  def execute(inputRdd: RDD[Any])(implicit ec: ExecutionContext): Future[SingleQueryIterateOutput]
  
  //TODO: This is a little inefficient.
  def executeNow(inputRdd: RDD[Any])(implicit ec: ExecutionContext): SingleQueryIterateOutput
    = Await.result(execute(inputRdd)(ec), Duration.Inf)
}

case class IndependentQueryExecutionBuilder(
    @Nullable private val stage: ErrorAnalysisStage,
    @Nullable private val cmd: String,
    @Nullable private val conf: SerializableWritable[HiveConf])
    extends QueryExecutionBuilder[IndependentQueryExecutionBuilder] {
  def this() = this(null, null, null)
  
  override def forStage(newStage: ErrorAnalysisStage): IndependentQueryExecutionBuilder = {
    new IndependentQueryExecutionBuilder(newStage, this.cmd, this.conf)
  }
  def forCmd(newCmd: String): IndependentQueryExecutionBuilder = {
    new IndependentQueryExecutionBuilder(this.stage, newCmd, this.conf)
  }
  def withConf(newConf: HiveConf): IndependentQueryExecutionBuilder = {
    new IndependentQueryExecutionBuilder(this.stage, this.cmd, new SerializableWritable(newConf))
  }
  
  override def build(): QueryExecution = {
    println("Building a single query in IndependentQueryExecution.") //TMP
    new IndependentQueryExecution(cmd, stage, conf.t)
  }
}

//NOTE: This only works in this process, nowhere else.
case class IndependentQueryExecution(cmd: String, stage: ErrorAnalysisStage, conf: HiveConf)
    extends QueryExecution {
  import QueryRunner._
  override def execute(inputRdd: RDD[Any])(implicit ec: ExecutionContext): Future[SingleQueryIterateOutput] = {
    QueryExecutionLock.synchronized {
      println("Executing a single query in IndependentQueryExecution.") //TMP
      //HACK: Hive requires some thread-local state to be initialized.
      //TODO: This might require all of this work to be done in a separate
      // thread.
      SessionState.start(conf)
      
      val sem = QueryRunner.doSemanticAnalysis(cmd, stage, conf)
      require(sem.isDefined) //FIXME
      val executionTask = sem.get.getRootTasks().get(0)
      val sourceOperators = getSourceOperators(sem.get)
      insertInputRdd(inputRdd, sourceOperators)
      QueryRunner.initializeOperatorTree(executionTask)
      val (output, oi) = QueryRunner.executeOperatorTree(executionTask, sourceOperators)
      QueryRunner.collectSingleQueryOutput(output, oi)
    }
  }
}

//HACK: Shark (really, Hive) does not support multiple concurrent queries.
// Fortunately we can "execute" a Shark query and get back a lazy RDD containing
// its result, then collect all such RDDs in parallel.  But the local execution
// (e.g. building the operator graph) must be done sequentially.  This is
// enforced in BlinkDB by convention: You must synchronize on this object
// when performing semantic analysis or executing a query.
object QueryExecutionLock

class SharedOperatorTreeQueryExecutionBuilder(
    @Nullable private val stage: ErrorAnalysisStage,
    @Nullable private val executionTask: Task[_ <: Serializable],
    @Nullable private val sourceOps: Seq[shark.execution.Operator[_]])
    extends QueryExecutionBuilder[SharedOperatorTreeQueryExecutionBuilder] {
  import SharedOperatorTreeQueryExecutionBuilder._
  
  def this() = this(null, null, null)
  
  override def forStage(newStage: ErrorAnalysisStage): SharedOperatorTreeQueryExecutionBuilder = {
    new SharedOperatorTreeQueryExecutionBuilder(newStage, this.executionTask, this.sourceOps)
  }
  //FIXME: This builder isn't really designed correctly any more.
  def forCommand(cmd: String, conf: HiveConf): SharedOperatorTreeQueryExecutionBuilder = {
    //HACK: Hive requires some thread-local state to be initialized.
    //TODO: This might require all of this work to be done in a separate
    // thread.
    SessionState.start(conf)
    
    require(stage != null)
    val sem = QueryRunner.doSemanticAnalysis(cmd, stage, conf)
    require(sem.isDefined) //FIXME
    val executionTask = sem.get.getRootTasks().get(0).asInstanceOf[Task[_ <: Serializable]]
    val sourceOperators = QueryRunner.getSourceOperators(sem.get)
    forTask(executionTask).withSourceOps(sourceOperators) //TMP
  }
  def forTask(executionTask: Task[_ <: Serializable]): SharedOperatorTreeQueryExecutionBuilder = {
    new SharedOperatorTreeQueryExecutionBuilder(this.stage, executionTask, this.sourceOps)
  }
  def withSourceOps(sourceOps: Seq[shark.execution.Operator[_]]): SharedOperatorTreeQueryExecutionBuilder = {
    new SharedOperatorTreeQueryExecutionBuilder(this.stage, this.executionTask, sourceOps)
  }
  
  override def build(): QueryExecution = {
    //FIXME: May need to check whether or not we've been serialized.  If not,
    // we might initialize this task multiple times, which Shark doesn't seem
    // to be happy about.
    println("Building a single query in SharedOperatorTreeQueryExecution.") //TMP
    QueryRunner.initializeOperatorTree(executionTask)
    new SharedOperatorTreeQueryExecution(executionTask, sourceOps)
  }
  
  def writeReplace(): Object = {
    val serializableSourceOps = 
    new SerializationProxy(stage, executionTask, serializableSourceOps)
  }
  
  def readResolve(): Object
    = throw new InvalidObjectException("Attempted to deserialize an object rather than its serialization proxy.")
}

object SharedOperatorTreeQueryExecutionBuilder {
  private class SerializationProxy extends Serializable {
    
  }
}

//TODO: We're doing some extra serialization of the Operators here.
case class SharedOperatorTreeQueryExecution(
    private val executionTask: Task[_ <: Serializable],
    private val sourceOps: Seq[shark.execution.Operator[_]])
    extends QueryExecution {
  import QueryRunner._
  override def execute(inputRdd: RDD[Any])(implicit ec: ExecutionContext): Future[SingleQueryIterateOutput] = {
    // We synchronize here because @executionTask and @sourceOps are shared
    // across threads.  We don't want them to be executed concurrently.  Even
    // if the ops themselves were thread-safe, the fact that they share an
    // intermediate input operator means object that we cannot execute them
    // concurrently with different intermediate input.  However, once we
    // extract output RDDs, we are free to collect the RDDs concurrently.
    QueryExecutionLock.synchronized {
      println("Executing a single query in SharedOperatorTreeQueryExecution.") //TMP
      insertInputRdd(inputRdd, sourceOps)
      val (output, oi) = QueryRunner.executeOperatorTree(executionTask, sourceOps)
      QueryRunner.collectSingleQueryOutput(output, oi)
    }
  }
}

object QueryRunner extends LogHelper {
  def logOperatorTree(sourceOperators: Seq[shark.execution.Operator[_]]): Unit = {
    if (!log.isDebugEnabled()) {
      return
    }
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
  
  def doSemanticAnalysis(cmd: String, stage: ErrorAnalysisStage, conf: HiveConf): Option[BaseSemanticAnalyzer] = {
    try {
      val command = new VariableSubstitution().substitute(conf, cmd)
      val context = new QueryContext(conf, false)
      context.setCmd(cmd)
      context.setTryCount(Integer.MAX_VALUE)

      val tree = ParseUtils.findRootNonNullToken((new ParseDriver()).parse(command, context))
      val sem = BlinkDbSemanticAnalyzerFactory.get(conf, tree, stage)

      //TODO: Currently I do not include configured SemanticAnalyzer hooks.
      sem.analyze(tree, context)
      sem.validate()
      Some(sem)
    } catch {
      case e: SemanticException => None
    }
  }
  
  def getSourceOperators(sem: BaseSemanticAnalyzer): Seq[shark.execution.Operator[_]] = {
    sem.getRootTasks()
      .map(_.getWork().asInstanceOf[SparkWork].terminalOperator.asInstanceOf[shark.execution.TerminalOperator])
      .flatMap(_.returnTopOperators())
      .distinct
  }
  
  def getSinkOperators(sourceOperators: Seq[shark.execution.Operator[_]]): Seq[shark.execution.Operator[_]] = {
    sourceOperators.flatMap(_.returnTerminalOperators()).distinct
  }
  
  def getIntermediateInputOperators(sem: BaseSemanticAnalyzer): Seq[IntermediateCacheOperator] = {
    //HACK
    Seq(sem.asInstanceOf[InputExtractionSemanticAnalyzer].intermediateInputOperator)
  }
  
  def getIntermediateRddScanOperators(sourceOps: Seq[shark.execution.Operator[_]]): Seq[RddScanOperator] = {
    val rddScanOps = sourceOps.flatMap(sourceOp => sourceOp.findOpsBelowMatching(_.isInstanceOf[RddScanOperator]))
    require(rddScanOps.size == 1, "Only queries with exactly 1 RddScanOperator are supported, but found %d.".format(rddScanOps.size))
    rddScanOps.map(_.asInstanceOf[RddScanOperator])
  }
  
  def insertInputRdd(inputRdd: RDD[_], sourceOps: Seq[shark.execution.Operator[_]]) {
    val intermediateOps = getIntermediateRddScanOperators(sourceOps)
    require(intermediateOps.size == 1)
    val intermediateOp = intermediateOps(0)
    intermediateOp.inputRdd = inputRdd
  }
  
  /** 
   * Initialize @executionTask for execution.  This needs to be done after it
   * is constructed or deserialized.
   */
  def initializeOperatorTree(executionTask: Task[_]): Unit = {
    require(executionTask.isInstanceOf[SparkTask], "Expected to execute a SparkTask, but instead found %s.".format(executionTask))
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
  def executeOperatorTree(executionTask: Task[_], sourceOperators: Seq[shark.execution.Operator[_]]): (RDD[Any], StructObjectInspector) = {
    val sinkOperators: Seq[shark.execution.Operator[_]] = getSinkOperators(sourceOperators)
    logOperatorTree(sourceOperators)
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
      .collectFuture()(ec)
    rawOutputsFuture.map(rawOutputs => {
      val numRows = rawOutputs.size
      val numFields = if (numRows > 0) rawOutputs(0).size else 0
      SingleQueryIterateOutput(rawOutputs, numRows, numFields)
    })
  }
}