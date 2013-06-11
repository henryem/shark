package blinkdb.util

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FunSuite
import spark.SparkContext
import spark.RDD
import akka.dispatch.Await
import akka.util.Duration

class RddUtilsSuite extends FunSuite with ShouldMatchers {
  test("randomlyPermute simple test") {
    val data = 0 until 100
    val numTests = 100
    val parallelism = 4
    RddUtils.runAndStop(new SparkContext("local[%d]".format(4), "test"), sc => {
      val rdd = sc.parallelize(data, parallelism)
      val confidenceLevel = .95
      val proportionDeclaredDependent = Seq.tabulate(numTests)(seed => {
        val permutation = RddUtils.randomlyPermute(rdd, seed).collect
        if (MathUtils.kendallTauTest(data, permutation, confidenceLevel)) 1 else 0
      }).sum.toDouble / numTests
      proportionDeclaredDependent should be < (1 - confidenceLevel)
    })
  }

}