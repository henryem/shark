package shark.execution
import spark.RDD
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import shark.memstore.TableStorage

/**
 * Wraps an existing RDD and passes it through to children operators.  Note
 * that an RDD produced by a CacheSinkOperator cannot be used directly here,
 * since each partition is stored in a serialized object.  See
 * TableScanOperator for an example of deserializing a cached RDD produced by
 * CacheSinkOperator.
 * 
 * Note: This really should be a TopOperator, but for now I am using it
 * in a slightly hacky way in the middle of an operator graph, and it is more
 * convenience to implement it as a unary operator.  The parents of this
 * operator are just ignored, except that they are used to populate its
 * ObjectInspector.
 * 
 * TODO: Add documentation.  For now this is to be used only in BlinkDB.
 * TODO: Use RddScanOperator in a more Shark-idiomatic way.  For now it is simply
 *   instantiated using new.
 */
class RddScanOperator()
    extends UnaryOperator[org.apache.hadoop.hive.ql.exec.ForwardOperator] {
  //TODO: Make this a constructor argument.  Unfortunately it seems that
  // Shark wants a no-args constructor.
  @transient var inputRdd: RDD[_] = _
  
  override def execute(): RDD[_] = inputRdd
  
  override def processPartition(split: Int, iter: Iterator[_]) =
    throw new UnsupportedOperationException("RddScanOperator.processPartition()")
  
  override def preprocessRdd(rdd: RDD[_]): RDD[_] =
    throw new UnsupportedOperationException("RddScanOperator.preprocessRdd()")

  override def postprocessRdd(rdd: RDD[_]): RDD[_] =
    throw new UnsupportedOperationException("RddScanOperator.postprocessRdd()")
}
