package shark.execution

import java.util.Properties

import scala.collection.Iterator
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

import shark.execution.serialization.OperatorSerializationWrapper
import shark.memstore.ColumnarSerDe
import shark.memstore.ColumnBuilderCreateFunc
import shark.memstore.RDDSerializer
import shark.memstore.TableStorage
import spark.storage.StorageLevel
import spark.RDD
import spark.SparkException

/** Caches an RDD in the middle of an operator graph. */
class IntermediateCacheOperator() extends UnaryOperator[org.apache.hadoop.hive.ql.exec.ForwardOperator] {
  
  // Create a local copy of hconf and hiveSinkOp so we can XML serialize it.
  @BeanProperty var localHiveOp: org.apache.hadoop.hive.ql.exec.ForwardOperator = _
  @BeanProperty var localHconf: HiveConf = _

  override def initializeOnMaster() {
    localHconf = super.hconf
    // Set parent to null so we won't serialize the entire query plan.
    hiveOp.setParentOperators(null)
    hiveOp.setChildOperators(null)
    hiveOp.setInputObjInspectors(null)
    localHiveOp = hiveOp
  }

  override def initializeOnSlave() {
    localHiveOp.initialize(localHconf, Array(objectInspector))
  }
  
  override def execute(): RDD[_] = {
    logInfo("execute") //TMP
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null

    //TMP
//    try {
//      val inputCollected = inputRdd.collect //TMP
//    } catch {
//      case e: SparkException => {
//        println(e)
//      }
//    }
    
    val op = OperatorSerializationWrapper(this)

    // Serialize the RDD on all partitions, then immediately deserialize it.
    // This is necessary because Hive reuses objects when iterating over rows,
    // so simply caching the RDD results in a bunch of pointers to the same row
    // object.
    val rdd = inputRdd.mapPartitionsWithIndex { case(split, iter) =>
      op.initializeOnSlave()

      //FIXME: Figure out what SerDe to use here.
      val serde = new ColumnarSerDe(ColumnBuilderCreateFunc.uncompressedArrayFormat)
      serde.objectInspector = op.objectInspector.asInstanceOf[StructObjectInspector]
      //FIXME: In CacheSinkOperator this uses the output table properties.  Here
      // we don't have access to an output table, so I'm not sure what to do.
      // Hopefully this will just work.
      serde.initialize(op.hconf, new Properties())

      val rddSerializier = new RDDSerializer(serde)
      val singletonSerializedIterator = rddSerializier.serialize(iter, op.objectInspector)
      if (singletonSerializedIterator.hasNext) {
        val tableStorage = singletonSerializedIterator.next.asInstanceOf[TableStorage]
        // Immediately deserialize.
        tableStorage.iterator
      } else {
        Iterator()
      }
    }
    rdd.persist(StorageLevel.MEMORY_AND_DISK) //FIXME: Make parametric.
//    val collected = rdd.collect //TMP
    rdd
  }
  
  override def processPartition(split: Int, iter: Iterator[_]) =
    throw new UnsupportedOperationException("IntermediateCacheOperator.processPartition()")
  
  override def preprocessRdd(rdd: RDD[_]): RDD[_] =
    throw new UnsupportedOperationException("IntermediateCacheOperator.preprocessRdd()")

  override def postprocessRdd(rdd: RDD[_]): RDD[_] =
    throw new UnsupportedOperationException("IntermediateCacheOperator.postprocessRdd()")
}