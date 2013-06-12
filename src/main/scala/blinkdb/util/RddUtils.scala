package blinkdb.util

import spark.RDD
import spark.TaskContext
import spark.Partition
import spark.Partitioner
import spark.Dependency
import spark.OneToOneDependency
import java.util.Random
import spark.SparkContext._
import spark.SparkContext
import spark.HashPartitioner

object RddUtils {
  def mean(rdd: RDD[Double]): Double = {
    val zero = (0, 0.0)
    val reducer = {(aggregate: (Int, Double), row: Double) => (aggregate._1 + 1, aggregate._2 + row)}
    val combiner = {(aggregateA: (Int, Double), aggregateB: (Int, Double)) => (aggregateA._1 + aggregateB._1, aggregateA._2 + aggregateB._2)}
    val (count, sum) = rdd.aggregate(zero)(reducer, combiner)
    require(count > 0)
    sum / count
  }
  
  /**
   * Randomly permute the elements of @rdd, distributing the resulting elements
   * randomly among the existing partitions of @rdd.
   */
  def randomlyPermute[D: ClassManifest](rdd: RDD[D], seed: Int): RDD[D]
    = randomlyPermute(rdd, rdd.partitions.size, seed)
  
  /**
   * Randomly permute the elements of @rdd, distributing the resulting elements
   * randomly among @numPartitions partitions.
   */
  def randomlyPermute[D: ClassManifest](rdd: RDD[D], numPartitions: Int, seed: Int): RDD[D] = {
    val random = new Random(seed)
    val shuffleSeed = random.nextInt
    val postShufflePermuteSeed = random.nextInt
    rdd.mapPartitionsWithIndex((partitionIdx, partition) => {
        val partitionRandom = new Random(shuffleSeed + partitionIdx)
        partition.map(item => (partitionRandom.nextInt % numPartitions, item))
      })
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex((partitionIdx, partition) => {
        //FIXME: This post-partitioning shuffle seems unnecessarily expensive -
        // there ought to be a way to have Spark do the shuffle online.
        val partitionRandom = new scala.util.Random(postShufflePermuteSeed + partitionIdx)
        partitionRandom.shuffle(partition.map(_._2).toSeq).iterator
      })
  }
  
  /** Run @f, passing it @sc, and then destroy @sc.  Useful for testing. */
  def runAndStop[D](sc: SparkContext, f: SparkContext => D): D = {
    try {
      f(sc)
    } finally {
      sc.stop()
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
    }
  }
  
  implicit def toRddPartitioningOps[D: ClassManifest](rdd: RDD[D]): RddPartitioningOps[D] = {
    new RddPartitioningOps(rdd)
  }
  
  class RddPartitioningOps[D: ClassManifest](wrappedRdd: RDD[D]) {
    /**
     * Get an RDD containing only partition @partitionIdx from @wrappedRdd.
     * The result has only 1 partition.
     */
    def getSinglePartition(partitionIdx: Int): RDD[D] = {
      new PartitionFilteredRdd(wrappedRdd, idx => idx == partitionIdx)
    }
  }
  
  //TODO: Unit test.
  /**
   * An RDD containing only partitions from @parent whose indices match
   * @partitionFilter.
   */
  class PartitionFilteredRdd[D: ClassManifest](
      @transient private val parent: RDD[D],
      @transient private val partitionFilter: Int => Boolean)
      extends RDD[D](parent) {
    override def compute(partition: Partition, context: TaskContext): Iterator[D]
      = firstParent[D].iterator(partition, context)
    
    override protected def getPartitions: Array[Partition] = {
      (0 until parent.partitions.size)
        .filter(partitionFilter)
        .map(idx => parent.partitions(idx))
        .toArray
    }
    
    override protected def getDependencies: Seq[Dependency[_]]
      = List(new OneToOneDependency(parent))
    
    override protected def getPreferredLocations(partition: Partition): Seq[String]
      = parent.preferredLocations(partition)
    
    override val partitioner = parent.partitioner
  }
}