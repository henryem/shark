package shark.execution.serialization

import java.io.Serializable
import org.apache.hadoop.hive.ql.exec.Operator
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.commons.lang.SerializationUtils
import javax.annotation.concurrent.NotThreadSafe
import javax.annotation.Nullable

/**
 * A serializable wrapper for a Hive Operator.  Operator serialization is
 * broken in several ways, and implementations have several cute workarounds:
 *  1. Operators cannot be ordinarily be serialized with their object
 *     inspectors, since object inspectors are not serializable.
 *     Implementations will attempt to work around this by removing the
 *     object inspectors and serializing them separately.  This in turn leads
 *     to a thread-safety issue, since serialization entails modification;
 *     implementations must specify a way of dealing with this.
 *  2. Operators are sometimes used as keys in HashMaps or in other ways that
 *     rely on sane implementations of equals() and hashcode().  Unfortunately,
 *     Hive's operators do not actually implement equals() and hashcode(), so
 *     they rely on reference equality, which is broken when an object is
 *     serialized and deserialized.  Implementations attempt to work around
 *     this by at least ensuring that the object inside the wrapper is
 *     reference-equal to the originally-wrapped object unless the wrapper
 *     has been deserialized.  That is, "new WrapperImpl(x).value == x".
 *  3. Operators claim to be Java-serializable but they really need to be
 *     serialized by java.beans.XMLEncoder.
 */
trait HiveOperatorWrapper[T <: Operator[_]] extends Serializable {
  def value: T
}

/**
 * Static factory methods for creating serializable wrappers around Hive
 * Operators.
 * 
 * NOTE: Currently only FileSinkOperator is supported.  Other operators may
 * require information from parent operators, which makes serialization tricky.
 */
object HiveOperatorSerialization {
  val lock = new Object
  
  @NotThreadSafe
  def serialize[T <: Operator[_]](hiveOp: T, hconf: SerializableHiveConf, useCompression: Boolean): HiveOperatorWrapper[T] = {
    val opBytes = lock.synchronized {
      SerializationUtils.serialize(new SerializableHiveOperator(hiveOp, hconf, useCompression))
    }
    new SerializedHiveOperator(opBytes, hiveOp)
  }
  
  def serializeLazily[T <: Operator[_]](hiveOp: T, hconf: SerializableHiveConf, useCompression: Boolean): HiveOperatorWrapper[T] = {
    new SerializableHiveOperator(hiveOp, hconf, useCompression)
  }
}

/**
 * A pre-serialized Hive Operator.  This is useful because serializing an
 * operator requires modifying it temporarily, so care must be taken to avoid
 * using the operator while it is being serialized.  Serializing greedily
 * makes it easier to do this.
 * 
 * To construct an instance of this class, use HiveOperatorSerialization.serialize()
 * while holding the lock object HiveOperatorSerialization.lock.  In addition,
 * any code that potentially uses an operator x concurrently with a call to
 * serialize(x) needs to hold the same lock.  Otherwise it risks seeing
 * undefined modifications to the operator.
 */
class SerializedHiveOperator[T <: Operator[_]](
    private val opBytes: Array[Byte],
    //NOTE: This field is null in any wrapper that is a product of deserialization.
    @Nullable @transient private val originalOp: T)
    extends HiveOperatorWrapper[T] with Serializable {
  @transient lazy val value: T = {
    if (originalOp != null) {
      originalOp
    } else {
      SerializationUtils.deserialize(opBytes).asInstanceOf[SerializableHiveOperator[T]].value      
    }
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
    private var hconf: SerializableHiveConf,
    private var useCompression: Boolean)
    extends HiveOperatorWrapper[T] with Serializable {
  override def value = hiveOp
  
  @transient lazy val lock = new Object
  
  private def writeObject(out: ObjectOutputStream) {
    lock.synchronized {
      val inputObjectInspectors = hiveOp.getInputObjInspectors()
      
      //FIXME: Parents and children are NOT serialized.  This breaks the
      // contract of serialization, but for now the neighbors are not needed
      // in any usage of this wrapper.  Typically neighbors are only used to
      // populate an operator's input ObjectInspectors, and we do that
      // manually here.
      val parentOperators = hiveOp.getParentOperators()
      val childOperators = hiveOp.getChildOperators()
      
      try {
        hiveOp.setInputObjInspectors(null)
        hiveOp.setParentOperators(null)
        hiveOp.setChildOperators(null)
        
        out.writeObject(new SerializableOiFreeOperator(hiveOp, useCompression))
        out.writeObject(hconf)
        out.writeObject(new SerializableObjectInspectors(inputObjectInspectors))
      } finally {
        hiveOp.setInputObjInspectors(inputObjectInspectors)
        hiveOp.setParentOperators(parentOperators)
        hiveOp.setChildOperators(childOperators)
      }
    }
  }
  
  private def readObject(in: ObjectInputStream) {
    hiveOp = in.readObject().asInstanceOf[SerializableOiFreeOperator[T]].value
    val deserializedHconf = in.readObject().asInstanceOf[SerializableHiveConf].value
    val inputObjectInspectors = in.readObject().asInstanceOf[SerializableObjectInspectors[ObjectInspector]].value
    hiveOp.initialize(deserializedHconf, inputObjectInspectors)
  }
}

/** 
 * A serialization wrapper for a Hive Operator that includes only XML-
 * serializable fields.  In particular, the operator should not include any
 * ObjectInspectors, as those are not XML-serializable.
 * 
 * Use XmlSerializer.getUseCompression(hconf) on the operator's associated
 * HiveConf to populate @useCompression.
 */
class SerializableOiFreeOperator[T <: Operator[_]](
    private var op: T,
    private var useCompression: Boolean)
    extends Serializable {
  def value = op

  private def readObject(in: ObjectInputStream) {
    val opBytes = in.readObject().asInstanceOf[Array[Byte]]
    op = XmlSerializer.deserialize(opBytes)
    useCompression = in.readBoolean()
  }

  private def writeObject(out: ObjectOutputStream) {
    val opBytes = XmlSerializer.serialize(op, useCompression)
    out.writeObject(opBytes) //TODO: Can we use write() instead?  Is there overhead from using writeObject()?
    out.writeBoolean(useCompression)
  }
}

class SerializableObjectInspector[O <: ObjectInspector](private var oi: O) extends Serializable {
  def value = oi

  private def readObject(in: ObjectInputStream) {
    val oiBytes = in.readObject().asInstanceOf[Array[Byte]]
    oi = KryoSerializer.deserialize(oiBytes)
  }

  private def writeObject(out: ObjectOutputStream) {
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

  private def readObject(in: ObjectInputStream) {
    val oisBytes = in.readObject().asInstanceOf[Array[Byte]]
    ois = KryoSerializer.deserialize(oisBytes)
  }

  private def writeObject(out: ObjectOutputStream) {
    val oisBytes = KryoSerializer.serialize(ois)
    out.writeObject(oisBytes)
  }
}

/**
 * Some of Hive's Operator descriptors (e.g. GroupByDesc for GroupByOperator)
 * claim to be Java-serializable but really aren't.  They need to be serialized
 * by a java.bean.XMLEncoder instead.  This is a wrapper that makes such an
 * object Java-serializable.
 * 
 * Use XmlSerializer.getUseCompression(hconf) on the operator's associated
 * HiveConf to populate @useCompression.
 */
class SerializableOperatorDescriptor[T](
    private var opDesc: T,
    private var useCompression: Boolean)
    extends Serializable {
  def value = opDesc
  
  private def readObject(in: ObjectInputStream) {
    val opDescBytes = in.readObject().asInstanceOf[Array[Byte]]
    opDesc = XmlSerializer.deserialize(opDescBytes)
    useCompression = in.readBoolean()
  }

  private def writeObject(out: ObjectOutputStream) {
    val opDescBytes = XmlSerializer.serialize(opDesc, useCompression)
    out.writeObject(opDescBytes)
    out.writeBoolean(useCompression)
  }
}