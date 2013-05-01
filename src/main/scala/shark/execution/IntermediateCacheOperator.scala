package shark.execution

import java.util.Properties
import scala.collection.Iterator
import scala.reflect.BeanProperty
import org.apache.hadoop.hive.conf.HiveConf
import shark.execution.serialization.OperatorSerializationWrapper
import shark.memstore.ColumnBuilderCreateFunc
import shark.memstore.ColumnarSerDe
import shark.memstore.ColumnarStructObjectInspector
import shark.memstore.RDDSerializer
import shark.memstore.TableStorage
import spark.storage.StorageLevel
import spark.RDD
import spark.SparkException
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import blinkdb.ColumnarObjectInspectingForwardOperator

/** 
 * Caches an RDD in the middle of an operator graph.  Currently this is
 * intended for use only as a parent of an RddScanOperator, though in principle
 * it could be used to cache any intermediate RDD.
 * 
 * NOTE: This operator must be paired with a Hive Operator produced by
 * IntermediateCacheOperator.makePartnerHiveOperator().
 */
class IntermediateCacheOperator() extends UnaryOperator[org.apache.hadoop.hive.ql.exec.ForwardOperator] {
  
  override def execute(): RDD[_] = {
    require(parentOperators.size == 1)
    val inputRdd = executeParents().head._2
    
    val op = OperatorSerializationWrapper(this)

    // Serialize the RDD on all partitions, then cache it and immediately
    // deserialize it.
    // This is necessary because Hive reuses objects when iterating over rows,
    // so simply caching the RDD results in a bunch of pointers to the same row
    // object.  Deserializing before caching also does not work; the reason is
    // probably similar, though I am not quite sure why.
    val rdd = inputRdd.mapPartitionsWithIndex { case(split, iter) =>
      op.initializeOnSlave()

      val serde = new ColumnarSerDe(ColumnBuilderCreateFunc.uncompressedArrayFormat)
      serde.objectInspector = ColumnarObjectInspectingForwardOperator.makeColumnarObjectInspector(
          op.objectInspector.asInstanceOf[StructObjectInspector])
      serde.initialize(op.hconf, new Properties())

      val rddSerializier = new RDDSerializer(serde)
      val singletonSerializedIterator = rddSerializier.serialize(iter, op.objectInspector)
      singletonSerializedIterator
    }
    rdd.persist(StorageLevel.MEMORY_AND_DISK) //FIXME: Make parametric.
    val deserializedRdd = rdd.mapPartitions( iter => {
      if (iter.hasNext) {
        iter.next.asInstanceOf[TableStorage].iterator
      } else {
        Iterator()
      }
    })
    deserializedRdd
  }
  
  
  
  override def processPartition(split: Int, iter: Iterator[_]) =
    throw new UnsupportedOperationException("IntermediateCacheOperator.processPartition()")
  
  override def preprocessRdd(rdd: RDD[_]): RDD[_] =
    throw new UnsupportedOperationException("IntermediateCacheOperator.preprocessRdd()")

  override def postprocessRdd(rdd: RDD[_]): RDD[_] =
    throw new UnsupportedOperationException("IntermediateCacheOperator.postprocessRdd()")
}

object IntermediateCacheOperator {
  def makePartnerHiveOperator() = {
    new ColumnarObjectInspectingForwardOperator()
  }
}