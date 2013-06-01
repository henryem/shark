package shark.execution
import spark.RDD
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import blinkdb.execution.ColumnarObjectInspectingForwardOperator

/**
 * Wraps an existing RDD and passes it through to children operators.  Note
 * that an RDD produced by a MemoryStoreSinkOperator cannot be used directly here,
 * since each partition is stored in a serialized object.  See
 * TableScanOperator for an example of deserializing a cached RDD produced by
 * MemoryStoreSinkOperator.  IntermediateCacheOperator does not have this problem.
 * 
 * NOTE: The parents of this operator are just ignored, except that they are
 * used to populate its ObjectInspector.  For now it is not suitable for use
 * as a top operator, since some code unfortunately relies on such operators
 * actually inheriting from TopOperator, and the same code would cause problems
 * when this operator is used in the middle of a graph.
 * 
 * NOTE: This operator must be paired with a Hive Operator produced by
 * RddScanOperator.makePartnerHiveOperator().
 * 
 * TODO: Add documentation.  For now this is to be used only in BlinkDB.
 * TODO: Use RddScanOperator in a more Shark-idiomatic way.  For now it is simply
 *   instantiated using new.
 */
class RddScanOperator extends UnaryOperator[org.apache.hadoop.hive.ql.exec.ForwardOperator] {
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

object RddScanOperator {
  def makePartnerHiveOperator() = {
    //TODO: It is not obvious that RddScanOperator produces ColumnarStructs as
    // output rows.  Refactor so this is more intuitive.
    new ColumnarObjectInspectingForwardOperator()
  }
}