package blinkdb.execution

import org.apache.hadoop.hive.ql.exec.ForwardOperator
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import shark.memstore2.ColumnarStructObjectInspector
import org.apache.hadoop.conf.Configuration

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