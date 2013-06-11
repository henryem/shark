package blinkdb.util

import java.util.Random
import cern.jet.random.Normal
import cern.jet.random.engine.DRand

object MathUtils {
  def quantile[I](quantile: Double)(data: Seq[I])(implicit ord: Ordering[I]): I = {
    require(0 <= quantile && quantile <= 1)
    val rank = math.min(data.size-1, math.round(quantile*data.size).toInt)
    quickSelect(rank, data, new Random(012)) //FIXME
  }
  
  def quickSelect[I](rank: Int, data: Seq[I], random: Random)(implicit ord: Ordering[I]): I = {
    val dataSize = data.size
    require(0 <= rank && rank < dataSize, "rank %d outside data range".format(data.size))
    val pivot = data(random.nextInt(dataSize))
    val smaller = data.filter(datum => ord.lt(datum, pivot))
    val larger = data.filter(datum => ord.gt(datum, pivot))
    val smallerSize = smaller.size
    val largerSize = larger.size
    val tiedSize = dataSize - smallerSize - largerSize
    if (smallerSize > rank) {
      quickSelect(rank, smaller, random)
    } else if (smallerSize + tiedSize <= rank) {
      quickSelect(rank - smallerSize - tiedSize, larger, random)
    } else {
      require(tiedSize > 0)
      require(smallerSize + tiedSize >= rank)
      pivot
    }
  }
  
  /** 
   * Divide @numThings evenly among @numBuckets, with ties going to earlier
   * buckets.  For example, divideAmong(5, 3) returns the sequence (2, 2, 1).
   */
  def divideAmong(numThings: Int, numBuckets: Int): Seq[Int] = {
    (0 until numBuckets)
      .map(bucketIdx => {
        (numThings + numBuckets - bucketIdx - 1) / numBuckets
      })
  }
  
  /** 
   * The number of net rank concordances between @a and @b, i.e. the number
   * of (i,j) pairs such that !((a(i) > a(j)) ^ (b(i) > b(j))).  This is useful
   * in Kendall's test for independence. 
   */
  def netConcordances[T <% Ordered[T], U <% Ordered[U]](a: Seq[T], b: Seq[U]): Int = {
    val n = a.size
    require(b.size == n)
    require(n > 1)
    (1 to n-1).map(i => (0 to i-1).map(j => a(i).compareTo(a(j)) * b(i).compareTo(b(j))).sum).sum
  }
  
  /**
   * Returns true if the probability that a Normal(nullMean, nullVariance)
   * distribution generates an observation at least as extreme as @realization
   * is less than 1-@confidenceLevel.  VERY roughly, this means that the null
   * hypothesis that @realization came from a Normal(nullMean, nulLVariance)
   * distribution is probably not true.
   */
  def twoSidedNormalTest(realization: Double, nullMean: Double, nullVariance: Double, confidenceLevel: Double): Boolean = {
    val normal = new Normal(nullMean, math.sqrt(nullVariance), new DRand(0))
    val cdfVal = normal.cdf(realization)
    val singleTailRejectionRegion = (1 - confidenceLevel) / 2
    cdfVal < singleTailRejectionRegion || 1 - singleTailRejectionRegion < cdfVal
  }
  
  /**
   * Returns true if the probability that two random, independent ordered
   * sequences @a and @b would have at least as much rank correlation as @a
   * and @b is below 1-@confidenceLevel.  Roughly, if this returns true, then
   * the pairs @a(i) and @b(i) are probably dependent.
   */
  def kendallTauTest[T <% Ordered[T], U <% Ordered[U]](a: Seq[T], b: Seq[U], confidenceLevel: Double): Boolean = {
    require(0 <= confidenceLevel && confidenceLevel <= 1)
    val numNetConcordances = netConcordances(a, b)
    val n = a.size
    val kendallTau = numNetConcordances.toDouble / (n*(n-1)/2)
    twoSidedNormalTest(kendallTau, 0, (4*n+10).toDouble/(9*n*n-9*n), confidenceLevel)
  }
}