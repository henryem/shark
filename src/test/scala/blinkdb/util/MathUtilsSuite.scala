package blinkdb.util

import org.scalatest.FunSuite
import java.util.Random

class MathUtilsSuite extends FunSuite {
  test("QuickSelect simple test") {
    val data = Seq(1, 0, 2, 4, 7, 2)
    for (
        test <- Seq((0, 0), (1, 1), (2, 2), (3, 2), (4, 4), (5, 7));
        seed <- (0 until 100)) {
      val (rank, expectedValue) = test
      assert(MathUtils.quickSelect(rank, data, new Random(seed)) === expectedValue, "value for rank %d".format(rank))
    }
  }
}