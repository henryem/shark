package shark.execution

import shark.LogHelper
import spark.RDD

trait PartitionProcessor extends Serializable with LogHelper {
  def processPartition(split: Int, iter: Iterator[_]): Iterator[_]
}

object PartitionProcessor extends LogHelper {
  
  /**
   * Convenience function to map a PartitionProcessor over an RDD, with some
   * extra logging.
   */
  def executeProcessPartition(
      processor: PartitionProcessor,
      rdd: RDD[_],
      opDescription: String,
      objectInspectorsDescription: String):
      RDD[_] = {
    rdd.mapPartitionsWithIndex({ case(split, partition) =>
      processor.logDebug("Started executing mapPartitions for operator: " + opDescription)
      processor.logDebug("Input object inspectors: " + objectInspectorsDescription)
      val newPart = processor.processPartition(split, partition)
      processor.logDebug("Finished executing mapPartitions for operator: " + opDescription)
      newPart
    })
  }
  
  def identity = new IdentityPartitionProcessor
}

class IdentityPartitionProcessor extends PartitionProcessor {
  override def processPartition(split: Int, iter: Iterator[_]) = iter
}
