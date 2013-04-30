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

/** Caches an RDD in the middle of an operator graph. */
class IntermediateCacheOperator() extends UnaryOperator[org.apache.hadoop.hive.ql.exec.ForwardOperator] {
  
  override def execute(): RDD[_] = {
    require(parentOperators.size == 1)
    val inputRdd = executeParents().head._2
    
    val op = OperatorSerializationWrapper(this)

    // Serialize the RDD on all partitions, then immediately deserialize it.
    // This is necessary because Hive reuses objects when iterating over rows,
    // so simply caching the RDD results in a bunch of pointers to the same row
    // object.
    val rdd = inputRdd.mapPartitionsWithIndex { case(split, iter) =>
      op.initializeOnSlave()

      //FIXME: Figure out what SerDe to use here.
      val serde = new ColumnarSerDe(ColumnBuilderCreateFunc.uncompressedArrayFormat)
      serde.objectInspector = ColumnarObjectInspectingForwardOperator.makeColumnarObjectInspector(
          op.objectInspector.asInstanceOf[StructObjectInspector])
      //FIXME: In CacheSinkOperator this uses the output table properties.  Here
      // we don't have access to an output table, so I'm not sure what to do.
      // Hopefully this will just work.
      serde.initialize(op.hconf, new Properties())

      val rddSerializier = new RDDSerializer(serde)
      val singletonSerializedIterator = rddSerializier.serialize(iter, op.objectInspector)
      singletonSerializedIterator
      //TMP
//      if (singletonSerializedIterator.hasNext) {
//        val tableStorage = singletonSerializedIterator.next.asInstanceOf[TableStorage]
//        // Immediately deserialize.
//        tableStorage.iterator
//      } else {
//        Iterator()
//      }
    }
//    rdd.foreach(_ => Unit) //TMP
    //TMP
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

/** A partner Hive Operator for any Operator that produces ColumnarStructs as outputs. */
//TODO: Move to its own file.
class ColumnarObjectInspectingForwardOperator extends org.apache.hadoop.hive.ql.exec.ForwardOperator {
  override def initializeOp(hconf: Configuration): Unit = {
    outputObjInspector = outputObjInspector match {
      case structOi: StructObjectInspector => ColumnarObjectInspectingForwardOperator.makeColumnarObjectInspector(structOi)
      case other => other
    }
    super.initializeOp(hconf)
  }
}

object ColumnarObjectInspectingForwardOperator {
  def makeColumnarObjectInspector(objectInspector: StructObjectInspector): ColumnarStructObjectInspector = {
    ColumnarStructObjectInspector.fromStructObjectInspector(objectInspector)
  }
}