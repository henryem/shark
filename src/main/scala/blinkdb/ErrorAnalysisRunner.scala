package blinkdb
import blinkdb.parse.InputExtractionSemanticAnalyzer
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer
import spark.RDD
import shark.LogHelper
import shark.execution.RddCacheHelper
import java.util.Random
import akka.dispatch.ExecutionContext
import java.util.concurrent.Executors
import akka.dispatch.Await
import akka.util.Duration
import akka.dispatch.Future

object ErrorAnalysisRunner extends LogHelper {
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
   *   TODO: Support columns without error quantifications, e.g. adding city as
   *     one of the selected columns in the above query.
   */
  def runForResult[E <: ErrorQuantification](
      cmd: String,
      errorQuantifier: ErrorQuantifier[E],
      conf: HiveConf):
      Option[ErrorAnalysis[E]] = {
    val inputRddOpt: Option[RDD[Any]] = makeInputRdd(cmd, conf)
    inputRddOpt.map(rdd => {
      //NOTE: We count the input here for two reasons:
      //  1. This forces the RDD to be in cache.  I'm not sure if Spark is not
      //     smart enough to avoid computing it multiple times if we call
      //     collect() concurrently, even if the RDD is marked as cached.
      //  2. Some error analysis operations require knowing the size of the
      //     input.
      val inputSize = rdd.count
      logInfo("Running analysis on a dataset of %d rows.".format(inputSize))
      val analysisStartTimeNanos = System.nanoTime()
      val random = new Random(123) //FIXME
      //TODO: May need to tune the thread pool.  This thread pool is mostly
      // going to be waiting on I/O, so we want to allow as many as can be
      // used.  The number of threads used may be on the order of 20,000.
      val executorService = Executors.newCachedThreadPool()
      implicit val ec = ExecutionContext.fromExecutorService(executorService)
      val bootstrapFuture = BootstrapRunner.doBootstrap(cmd, rdd, errorQuantifier, conf, random.nextInt)
      val diagnosticFuture = DiagnosticRunner.doDiagnostic(cmd, rdd, inputSize, errorQuantifier, conf, random.nextInt)
      val errorAnalysisFuture = bootstrapFuture.zip(diagnosticFuture).map({case (bootstrap, diagnostic) => ErrorAnalysis(bootstrap, diagnostic) })
      val errorAnalysis = Await.result(errorAnalysisFuture, Duration.Inf)
      executorService.shutdown()
      val analysisFinishTimeNanos = System.nanoTime()
      logInfo("Analysis took %d nanoseconds.".format(analysisFinishTimeNanos - analysisStartTimeNanos)) //TMP
      errorAnalysis
    })
  }
  
  /** 
   * Make an RDD containing the input to @cmd, suitable for insertion in @cmd
   * via an RddScanOperator.  A helper object for serializing @rdd is also
   * returned.
   * 
   * @return None if @cmd is not suitable for extracting input.
   */
  private def makeInputRdd(cmd: String, conf: HiveConf): Option[RDD[Any]] = {
    val semOpt = QueryRunner.doSemanticAnalysis(cmd, ErrorAnalysisStage.InputExtraction, conf, None)
    semOpt
      .filter(_.isInstanceOf[InputExtractionSemanticAnalyzer])
      .filter(_.asInstanceOf[SemanticAnalyzer].getParseContext().getQB().getIsQuery())
      .map(sem => {
        val intermediateInputOperators = QueryRunner.getIntermediateInputOperators(sem)
        QueryRunner.initializeOperatorTree(sem)
      
        //TODO: Handle more than 1 sink.
        require(intermediateInputOperators.size == 1)
        intermediateInputOperators(0).execute().asInstanceOf[RDD[Any]]
      })
  }
}