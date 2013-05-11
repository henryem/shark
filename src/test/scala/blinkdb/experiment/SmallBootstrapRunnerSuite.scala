package blinkdb.experiment

import org.scalatest.FunSuite
import edu.berkeley.blbspark.util.Statistics
import spark.SparkContext
import blinkdb.StandardDeviationErrorQuantifier
import blinkdb.ErrorAnalysisConf
import blinkdb.BootstrapConf

/** 
 * Not real unit tests - just runs the SmallBootstrapRunner procedures to help
 * figure out their performance.
 */
class SmallBootstrapRunnerSuite extends FunSuite {
  test("Diagnostic performance") {
    val sc = new SparkContext("local[5]", "test") //FIXME
    val numSplits = 10
    val originalData = sc.parallelize(0 until 300000, numSplits).map(_.toDouble)
    val diagnosticResult = SmallDiagnosticRunner.doSingleJobDiagnostic(
        originalData,
        Statistics.mean,
        StandardDeviationErrorQuantifier,
        ErrorAnalysisConf.default,
        sc,
        numSplits,
        012 //FIXME
        )
    println(diagnosticResult)
  }
  
  test("Bootstrap performance") {
    val sc = new SparkContext("local[5]", "test") //FIXME
    val numSplits = 5
    val bootstrapResult = SmallBootstrapRunner.doBroadcastBootstrap(
        (0 until 25000).map(_.toDouble),
        Statistics.mean,
        StandardDeviationErrorQuantifier,
        BootstrapConf.default,
        sc,
        numSplits,
        012 //FIXME
        )
    println(bootstrapResult)
  }
}