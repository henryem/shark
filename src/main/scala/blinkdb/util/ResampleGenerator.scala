package blinkdb.util
import spark.RDD
import edu.berkeley.blbspark.StratifiedBlb
import edu.berkeley.blbspark.WeightedItem
import shark.execution.RddCacheHelper
import edu.berkeley.blbspark.dist.BernoulliDistribution
import java.util.Random

object ResampleGenerator {
  /** 
   * Create @numResamples resamples from @originalRdd.  Each resample is a
   * simple random sample with replacement from @originalRdd, having size
   * @originalRdd.count.  The actual sampling process may only provide
   * an approximation to this sampling process.
   * 
   * @param originalRdd should be cached.
   */
  def generateResamples[I: ClassManifest](originalRdd: RDD[I], numResamples: Int, seed: Int): Seq[RDD[I]] = {
    val originalTableWithWeights: RDD[WeightedItem[I]] = originalRdd.map(toWeightedRow)
    //TODO: Use BLB instead of bootstrap here.
    val resamples: Seq[RDD[WeightedItem[I]]] = StratifiedBlb.createBootstrapResamples(
        originalTableWithWeights,
        numResamples,
        originalTableWithWeights.partitions.length,
        seed) //FIXME: Random seed here.
     val unweightedResamples = resamples.map(_.flatMap(fromWeightedRow)) //TMP
     println("Resample sizes: %s".format(unweightedResamples.map(_.count()))) //TMP
     unweightedResamples
  }
  
  /**
   * Create @numSubsamples subsamples from @originalRdd.  Each subsample is a
   * simple random sample with replacement from @originalRdd, having size
   * @subsampleSize.  The actual sampling process may only provide an
   * approximation to this sampling process.
   * 
   * @param originalRdd should be cached.
   */
  def generateSubsamples[I: ClassManifest](originalRdd: RDD[I], originalRddCacher: RddCacheHelper, numSubsamples: Int, subsampleSize: Int, seed: Int): Seq[RDD[I]] = {
    //TODO: Use a single partition for each subsample.
    //TODO: Avoid the call to count() here by getting the information upstream,
    // when this RDD was originally cached and collected.
    val subsampleProportion = subsampleSize.toDouble / originalRdd.count()
    //TODO: This method produces approximate sample sizes, not exact ones.
    // We may need to do exact sampling, since the subsample size can be very
    // small (e.g. 100) and originalRdd.count() can be arbitrarily large.
    //TODO: Since it is difficult to cache @originalRdd, I have reimplemented
    // subsampling in a way that does not require caching.  This implementation
    // does not do sampling without replacement to form the subsamples.  I
    // believe caching is required to do sampling without replacement
    // efficiently.  @originalRddCacher can be used for this purpose, but
    // currently it is unused.
    (0 until numSubsamples).map({subsampleIdx =>
      val numPartitions = originalRdd.partitions.size
      originalRdd.mapPartitionsWithIndex({(partitionIdx: Int, partition: Iterator[I]) =>
        val partitionSeed = seed + subsampleIdx*numPartitions + partitionIdx
        val random = new Random(partitionSeed)
        val bernoulli = new BernoulliDistribution(subsampleProportion, partitionSeed)
        partition.filter(d => bernoulli.sample())
      })
    })
  }
  
  private def toWeightedRow[I](row: I): WeightedItem[I] = {
    WeightedItem(row, 1.0)
  }
  
  private def fromWeightedRow[I](weightedRow: WeightedItem[I]): Iterator[I] = {
    //FIXME: This is copied from blbspark's WeightedRepeatingIterable.
    require(weightedRow.weight == math.round(weightedRow.weight))
    Iterator.empty.padTo(math.round(weightedRow.weight).asInstanceOf[Int], weightedRow.item)
  }
}
