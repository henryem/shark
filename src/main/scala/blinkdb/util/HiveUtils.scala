package blinkdb.util
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category

/** Assorted utilities for interfacing with Hive in BlinkDb. */
object HiveUtils {
  /** 
   * Convert @rawOutput, a single row produced by a Hive query, to a
   * sequence of numbers.  It is assumed that @rawOutput is a Struct and that
   * each of its fields is numeric.
   */
  def toNumericRow(rawOutput: Any, objectInspector: StructObjectInspector): Seq[Double] = {
    val struct = objectInspector.getStructFieldsDataAsList(rawOutput)
    val structFieldRefs = objectInspector.getAllStructFieldRefs()
    (0 until structFieldRefs.size).map({fieldIdx =>
      val fieldOi = objectInspector.getAllStructFieldRefs().get(fieldIdx).getFieldObjectInspector()
      require(fieldOi.getCategory() == Category.PRIMITIVE)
      val primitiveOi = fieldOi.asInstanceOf[PrimitiveObjectInspector]
      val primitiveFieldValue = primitiveOi.getPrimitiveJavaObject(struct.get(fieldIdx))
      primitiveToDouble(primitiveFieldValue)
    })
  }
  
  private def primitiveToDouble(primitive: Object): Double = {
    //FIXME: May want to allow more types.  Note that Hive's
    // PrimitiveObjectInspectorUtils.getDouble() is not suitable, since it
    // merrily converts Strings and other inappropriate types to doubles.
    primitive match {
      case d: java.lang.Double => d.asInstanceOf[java.lang.Double]
      case f: java.lang.Float => f.asInstanceOf[java.lang.Float].toDouble
      case i: java.lang.Integer => i.asInstanceOf[java.lang.Integer].toDouble
      case l: java.lang.Long => l.asInstanceOf[java.lang.Long].toDouble
      case other => throw new IllegalArgumentException("Unexpected aggregate value type for bootstrap: %s".format(other))
    }
  }
}