package shark.execution.serialization

import java.io.Serializable
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import org.apache.hadoop.hive.conf.HiveConf

//FIXME: This doesn't really serialize a HiveConf.  Unfortunately, HiveConf is
// not really serializable, even though it is marked so.  When the HiveConf
// is deserialized, its values are set to their defaults.  So we get a valid
// HiveConf object, but not the one we serialized.  For now this is just going
// to stay broken.
//TODO: This is the same as SerializableOiFreeOperator.  We could have a
// generic XmlSerializableToJavaSerializable wrapper instead.  The main
// problem is that there is no XmlSerializable interface, so the compiler
// would not check that XML serialization will work.  Also, we may need to
// do some special things to serialize HiveConf later.
class SerializableHiveConf(
    private var hconf: HiveConf,
    private var useCompression: Boolean)
    extends Serializable {
  def value = hconf
  
  private def readObject(in: ObjectInputStream) {
    val hconfBytes = in.readObject().asInstanceOf[Array[Byte]]
    hconf = XmlSerializer.deserialize(hconfBytes)
    useCompression = in.readBoolean()
  }

  private def writeObject(out: ObjectOutputStream) {
    val hconfBytes = XmlSerializer.serialize(hconf, useCompression)
    out.writeObject(hconfBytes) //TODO: Can we use write() instead?  Is there overhead from using writeObject()?
    out.writeBoolean(useCompression)
  }
}