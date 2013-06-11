package blinkdb.util

import org.scalatest.FunSuite
import java.util.Random
import org.scalatest.matchers.ShouldMatchers

class MathUtilsSuite extends FunSuite with ShouldMatchers {
  test("quickSelect simple test") {
    val data = Seq(1, 0, 2, 4, 7, 2)
    for (
        test <- Seq((0, 0), (1, 1), (2, 2), (3, 2), (4, 4), (5, 7));
        seed <- (0 until 100)) {
      val (rank, expectedValue) = test
      assert(MathUtils.quickSelect(rank, data, new Random(seed)) === expectedValue, "value for rank %d".format(rank))
    }
  }
  
  test("divideAmong simple test") {
    MathUtils.divideAmong(0, 1) should equal (Seq(0))
    MathUtils.divideAmong(1, 1) should equal (Seq(1))
    MathUtils.divideAmong(10, 1) should be (Seq(10))
    MathUtils.divideAmong(5, 7) should be (Seq(1, 1, 1, 1, 1, 0, 0))
    MathUtils.divideAmong(100, 3) should be (Seq(34, 33, 33))
    MathUtils.divideAmong(98, 3) should be (Seq(33, 33, 32))
    MathUtils.divideAmong(99, 3) should be (Seq(33, 33, 33))
    MathUtils.divideAmong(100, 3) should be (Seq(34, 33, 33))
  }
  
  test ("kendallTauTest rejects the independence hypothesis when there is strong dependence") {
    MathUtils.kendallTauTest(0 until 100, 0 until 100, .99) should be (true)
  }
  
  test ("kendallTauTest rarely rejects the independence hypothesis when there is really independence") {
    val numTests = 10000
    val confidenceLevel = .99
    val proportionRejecting = Seq.tabulate(numTests)(i => {
      val random = new scala.util.Random(i)
      if (MathUtils.kendallTauTest(random.shuffle(0 until 100), 0 until 100, confidenceLevel)) 1 else 0
    }).sum.toDouble / numTests
    proportionRejecting should be < (1 - confidenceLevel)
  }
}