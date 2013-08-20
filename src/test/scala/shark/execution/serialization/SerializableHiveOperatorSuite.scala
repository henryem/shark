package shark.execution.serialization

import org.apache.hadoop.hive.ql.exec._
import org.apache.hadoop.hive.ql.exec.{Operator => HiveOperator}
import org.scalatest.FunSuite
import org.apache.hadoop.hive.ql.plan._
import org.apache.commons.lang.SerializationUtils
import shark.LogHelper
import org.apache.hadoop.hive.conf.HiveConf

/**
 * Test suite for SerializableHiveOperator and related classes.
 * 
 * Currently this contains only sanity checks.  Ideally we would populate
 * each Hive operator with some standard values, serialize it, and then check
 * that the deserialized form is equal to the original.  However, Hive
 * operators provide (as far as I can tell) no easy way to construct them with
 * standard values, nor do they provide an equals() method.  So writing real
 * tests will involve some manual work.  Instead we leave all fields blank;
 * this could fail to catch problems with particular operators containing
 * non-serializable fields.
 */
class SerializableHiveOperatorSuite extends FunSuite with LogHelper {
  import SerializableHiveOperatorSuite._
  
  test("Preserialize operators using HiveOperatorSerialization.serialize(), and just ensure that no exceptions are thrown when the serializable object is accessed.") {
    OPERATORS.foreach({ op =>
      val preserialized = HiveOperatorSerialization.serialize(op, hconf, true)
      assert(preserialized.value != null)
    })
  }
  
  test("Preserialize operators using SerializedHiveOperator.serialize(), and just ensure that no exceptions are thrown when the serializable object is serialized, deserialized, and accessed.") {
    OPERATORS.foreach({ op =>
      val preserialized = HiveOperatorSerialization.serialize(op, hconf, true)
      val afterSerialization = SerializationUtils.deserialize(SerializationUtils.serialize(preserialized))
      assert(afterSerialization.isInstanceOf[SerializedHiveOperator[_]])
      assert(afterSerialization.asInstanceOf[SerializedHiveOperator[_]].value != null)
    })
  }
  
  test("Lazily serialize operators using SerializableHiveOperator, and just ensure that no exceptions are thrown when the serializable object is serialized, deserialized, and accessed.") {
    OPERATORS.foreach({ op =>
      val lazilySerialized = HiveOperatorSerialization.serializeLazily(op, hconf, true)
      val afterSerialization = SerializationUtils.deserialize(SerializationUtils.serialize(lazilySerialized))
      assert(afterSerialization.isInstanceOf[SerializableHiveOperator[_]])
      assert(afterSerialization.asInstanceOf[SerializableHiveOperator[_]].value != null)
    })
  }
}

object SerializableHiveOperatorSuite {
  private val hconf = new SerializableHiveConf(new HiveConf(), true)
  
  private val OPERATORS: Seq[HiveOperator[_ <: java.io.Serializable]] = Seq(
      new CollectDesc(),
      new ExtractDesc(),
      new ExtractDesc(),
      new FileSinkDesc(),
      new FilterDesc(),
      new ForwardDesc(),
      new GroupByDesc(),
      new HashTableDummyDesc(),
      new HashTableSinkDesc(),
      new JoinDesc(),
      new LateralViewForwardDesc(),
      new LateralViewJoinDesc(),
      new LimitDesc(),
      new MapJoinDesc(),
      new ReduceSinkDesc(),
      new ScriptDesc(),
      new SelectDesc(),
      new TableScanDesc(),
      new UDTFDesc(),
      new UnionDesc())
      .map(desc => OperatorFactory.get(desc))
}