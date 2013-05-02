package blinkdb
import blinkdb.parse.InputExtractionSemanticAnalyzer
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer
import spark.RDD
import shark.LogHelper
import shark.execution.RddCacheHelper
import java.util.Random

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
   *   TODO: Wrap the return value into a single object.
   *   TODO: Support columns without error quantifications, e.g. adding city as
   *     one of the selected columns in the above query.
   */
  def runForResult[E <: ErrorQuantification](
      cmd: String,
      errorQuantifier: ErrorQuantifier[E],
      conf: HiveConf):
      Option[ErrorAnalysis[E]] = {
    val inputRdd: Option[(RDD[Any], RddCacheHelper)] = makeInputRdd(cmd, conf)
    val random = new Random(123) //FIXME
    inputRdd.map(rdd => {
      ErrorAnalysis(
          BootstrapRunner.doBootstrap(cmd, rdd._1, errorQuantifier, conf, random.nextInt),
          DiagnosticRunner.doDiagnostic(cmd, rdd._1, rdd._2, errorQuantifier, conf, random.nextInt))
    })
  }
  
  /** 
   * Make an RDD containing the input to @cmd, suitable for insertion in @cmd
   * via an RddScanOperator.  A helper object for serializing @rdd is also
   * returned.
   * 
   * @return None if @cmd is not suitable for extracting input.
   */
  private def makeInputRdd(cmd: String, conf: HiveConf): Option[(RDD[Any], RddCacheHelper)] = {
    val sem = QueryRunner.doSemanticAnalysis(cmd, BootstrapStage.InputExtraction, conf, None)
    if (!sem.isInstanceOf[InputExtractionSemanticAnalyzer]
        || !sem.asInstanceOf[SemanticAnalyzer].getParseContext().getQB().getIsQuery()) {
      //TODO: With Sameer's SQL parser, this will be unnecessary - we can just
      // check whether the user explicitly asked for an approximation.  For now
      // we execute the bootstrap on anything that looks like a query.
      None
    } else {
      val intermediateInputOperators = QueryRunner.getIntermediateInputOperators(sem)
      QueryRunner.initializeOperatorTree(sem)
    
      //TODO: Handle more than 1 sink.
      require(intermediateInputOperators.size == 1)
      Some((intermediateInputOperators(0).execute().asInstanceOf[RDD[Any]], intermediateInputOperators(0).getRddCacher()))
    }
  }
}