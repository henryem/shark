package shark.execution.serialization

import java.io.Serializable
import org.apache.hadoop.hive.ql.exec.Operator
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.commons.lang.SerializationUtils
import javax.annotation.concurrent.NotThreadSafe

/**
 * A pre-serialized Hive Operator.  This is useful because serializing an
 * operator requires modifying it temporarily, so care must be taken to avoid
 * using the operator while it is being serialized.  Serializing greedily
 * makes it easier to do this.
 * 
 * TODO: Document locking scheme.
 */
class SerializedHiveOperator[T <: Operator[_]] private (private val opBytes: Array[Byte])
    extends Serializable {
  @transient lazy val value: T = SerializationUtils.deserialize(opBytes).asInstanceOf[SerializableHiveOperator[T]].value
}

object SerializedHiveOperator {
  val lock = new Object
  
  //TODO: Document.  Callers need to hold this.lock when using @hiveOp if it
  // might currently be getting serialized.
  @NotThreadSafe
  def serialize[T <: Operator[_]](hiveOp: T, useCompression: Boolean): SerializedHiveOperator[T] = {
    val opBytes = lock.synchronized {
      SerializationUtils.serialize(new SerializableHiveOperator(hiveOp, useCompression))
    }
    new SerializedHiveOperator(opBytes)
  }
}

/**
 * Wraps a Hive Operator for serialization.
 * 
 * Note: When you wrap a Hive Operator in an instance of this class, you MUST
 * hold this.lock before using that operator in any way.  This ugliness is
 * necessary because serializing a Hive Operator requires temporarily modifying
 * it in ways that will cause undefined behavior for any code that is touching
 * the operator.  Ideally we would just copy the thing before making these
 * modifications, but Operator provides no method for copying itself.
 * 
 * TODO: This might need to be a global lock, since operators might interact
 * with each other in ways that make it difficult to hold all of their locks.
 */
class SerializableHiveOperator[T <: Operator[_]](
    private var hiveOp: T,
    private var useCompression: Boolean)
    extends Serializable {
  
  val lock = new Object
  def value = hiveOp
  
  private def writeObject(out: ObjectOutputStream) {
    lock.synchronized {
      val inputObjectInspectors = hiveOp.getInputObjInspectors()
      hiveOp.setInputObjInspectors(null)
      //FIXME: Do we need to handle any other fields, like output object inspectors?
      // I don't understand how operator serialization really works, and I can't
      // find any documentation.
      
      out.defaultWriteObject()
      out.writeObject(new SerializableOiFreeOperator(hiveOp, useCompression))
      out.writeObject(new SerializableObjectInspectors(inputObjectInspectors))
      hiveOp.setInputObjInspectors(inputObjectInspectors)
    }
  }
  
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    hiveOp = in.readObject().asInstanceOf[T]
    val inputObjectInspectors = in.readObject().asInstanceOf[SerializableObjectInspectors[ObjectInspector]].value
    hiveOp.setInputObjInspectors(inputObjectInspectors)
  }
}

/** 
 * A serialization wrapper for a Hive Operator that includes only XML-
 * serializable fields.  In particular, the operator should not include any
 * ObjectInspectors, as those are not XML-serializable.
 */
class SerializableOiFreeOperator[T <: Operator[_]](
    private var op: T,
    private var useCompression: Boolean)
    extends Serializable {
  def value = op

  def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    val opBytes = in.readObject().asInstanceOf[Array[Byte]]
    op = XmlSerializer.deserialize(opBytes)
    useCompression = in.readBoolean()
  }

  def writeObject(out: ObjectOutputStream) {
    out.defaultWriteObject()
    val opBytes = XmlSerializer.serialize(op, useCompression)
    out.writeObject(opBytes) //TODO: Can we use write() instead?  Is there overhead from using writeObject()?
    out.writeBoolean(useCompression)
  }
}

class SerializableObjectInspector[O <: ObjectInspector](private var oi: O) extends Serializable {
  def value = oi

  def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    val oiBytes = in.readObject().asInstanceOf[Array[Byte]]
    oi = KryoSerializer.deserialize(oiBytes)
  }

  def writeObject(out: ObjectOutputStream) {
    out.defaultWriteObject()
    val oiBytes = KryoSerializer.serialize(oi)
    out.writeObject(oiBytes)
  }
}

/**
 * Typically object inspectors come in arrays.  This is a more convenient
 * serialization method than SerializableObjectInspector for wrapping the
 * whole thing in one shot.
 */
class SerializableObjectInspectors[O <: ObjectInspector](private var ois: Array[O]) extends Serializable {
  def value = ois

  def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    val oisBytes = in.readObject().asInstanceOf[Array[Byte]]
    ois = KryoSerializer.deserialize(oisBytes)
  }

  def writeObject(out: ObjectOutputStream) {
    out.defaultWriteObject()
    val oisBytes = KryoSerializer.serialize(ois)
    out.writeObject(oisBytes)
  }
}