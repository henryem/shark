package shark.execution.serialization

import org.scalatest.FunSuite
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.commons.lang.SerializationUtils
import org.scalatest.matchers.Matchers
import org.scalatest.matchers.ShouldMatchers

class SerializableHiveConfSuite extends FunSuite with ShouldMatchers {
  test("Ensure a simple HiveConf can be serialized and deserialized without exceptions using SerializableHiveConf.") {
    //FIXME: Probably need to have a hive-site.xml fixture for this.
    // Otherwise, the constructor will try to find a real one somewhere.
    val hiveConf: HiveConf = new HiveConf()
    val serializable = new SerializableHiveConf(hiveConf, true)
    val deserialized = SerializationUtils.deserialize(SerializationUtils.serialize(serializable))
    assert(deserialized.isInstanceOf[SerializableHiveConf])
    deserialized.asInstanceOf[SerializableHiveConf].value should not be null
  }
  
  //NOTE: SerializableHiveConf is broken because HiveConf is not really
  // XML-serializable.  This test won't pass until SerializableHiveConf is
  // fixed.
//  test("Ensure modified properties of a HiveConf are actually serialized and deserialized properly.") {
//    val TEST_KEY = "foo"
//    val TEST_VAL = "fooval"
//    
//    val hiveConf: HiveConf = new HiveConf()
//    hiveConf.set(TEST_KEY, TEST_VAL)
//    val serializable = new SerializableHiveConf(hiveConf, true)
//    val deserialized = SerializationUtils.deserialize(SerializationUtils.serialize(serializable))
//    deserialized.asInstanceOf[SerializableHiveConf].value.get(TEST_KEY) should equal (TEST_VAL)
//  }
}