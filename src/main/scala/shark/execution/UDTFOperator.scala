/*
 * Copyright (C) 2012 The Regents of The University California. 
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution

import java.util.{List => JavaList}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty
import org.apache.hadoop.hive.ql.exec.{UDTFOperator => HiveUDTFOperator}
import org.apache.hadoop.hive.ql.plan.UDTFDesc
import org.apache.hadoop.hive.ql.udf.generic.Collector
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspector,
  StandardStructObjectInspector, StructField, StructObjectInspector }
import shark.execution.serialization.SerializableObjectInspectors


class UDTFOperator extends SimpleUnaryOperator[HiveUDTFOperator] {
  override def makePartitionProcessor(): UDTFOperator.UDTFPartitionProcessor = {
    new UDTFOperator.UDTFPartitionProcessor(
        hiveOp.getConf(),
        new SerializableObjectInspectors(hiveOp.getInputObjInspectors()))
  }
}

object UDTFOperator {
  //HACK: This class should be private.  Currently, LateralViewJoinOperator reaches
    // intrusively into the internals of UDTFOperator and needs access to
    // makeExploder().
  class UDTFPartitionProcessor(
      private val conf: UDTFDesc,
      private val serializableObjectInspectors: SerializableObjectInspectors[ObjectInspector])
      extends PartitionProcessor {
    override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = {
      iter.flatMap(makeExploder())
    }
    
    //HACK: This should be private.  Currently, LateralViewJoinOperator reaches
    // intrusively into the internals of UDTFOperator and needs access to
    // makeExploder().
    def makeExploder(): Any => ArrayBuffer[java.lang.Object] = {
      val collector = new UDTFCollector
      conf.getGenericUDTF().setCollector(collector)
  
      // Make an object inspector [] of the arguments to the UDTF
      val soi = serializableObjectInspectors.value.toArray.head.asInstanceOf[StandardStructObjectInspector]
      val inputFields = soi.getAllStructFieldRefs()
  
      val udtfInputOIs = inputFields.map { case inputField =>
        inputField.getFieldObjectInspector()
      }.toArray
  
      val objToSendToUDTF = new Array[java.lang.Object](inputFields.size)
      conf.getGenericUDTF().initialize(udtfInputOIs)
      
      (row: Any) => {
        (0 until inputFields.size).foreach { case i =>
          objToSendToUDTF(i) = soi.getStructFieldData(row, inputFields.get(i))
        }
        conf.getGenericUDTF().process(objToSendToUDTF)
        collector.collectRows()
      }
    }
  }
}

class UDTFCollector extends Collector {

  var collected = new ArrayBuffer[java.lang.Object]

  override def collect(input: java.lang.Object) {
    // We need to clone the input here because implementations of
    // GenericUDTF reuse the same object. Luckily they are always an array, so
    // it is easy to clone.
    collected += input.asInstanceOf[Array[_]].clone
  }

  def collectRows() = {
    val toCollect = collected
    collected = new ArrayBuffer[java.lang.Object]
    toCollect
  }

}
