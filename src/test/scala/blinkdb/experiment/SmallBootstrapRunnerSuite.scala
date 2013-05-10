package blinkdb.experiment

import org.scalatest.FunSuite
import edu.berkeley.blbspark.util.Statistics
import spark.SparkContext
import blinkdb.StandardDeviationErrorQuantifier

class SmallBootstrapRunnerSuite extends FunSuite {
  test("Diagnostic performance") {
    val sc = new SparkContext("local[5]", "test") //FIXME
    val numSplits = 50
    val originalData = sc.parallelize(0 until 300000, numSplits).map(_.toDouble)
    val diagnosticResult = SmallBootstrapRunner.doDiagnostic(
        originalData,
        Statistics.mean,
        StandardDeviationErrorQuantifier,
        100,
        Seq(250, 500, 1000),
        100,
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
        (0 until 6400000).map(_.toDouble),
        Statistics.mean,
        StandardDeviationErrorQuantifier,
        100,
        sc,
        numSplits,
        012 //FIXME
        )
    println(bootstrapResult)
  }
}