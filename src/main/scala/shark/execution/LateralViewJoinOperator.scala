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

import java.nio.ByteBuffer
import java.util.ArrayList

import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.exec.{LateralViewJoinOperator => HiveLateralViewJoinOperator}
import org.apache.hadoop.hive.ql.plan.SelectDesc
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructObjectInspector}

import spark.RDD

import shark.execution.serialization.SerializableObjectInspectors

/**
 * LateralViewJoin is used only for LATERAL VIEW explode, which adds a new row per array element
 * in the array to be exploded. Each new row contains one of the array elements in a new field.
 * Hive handles this by having two branches in its plan, then joining their output (see diagram in
 * LateralViewJoinOperator.java). We put all the explode logic here instead.
 */
class LateralViewJoinOperator extends Operator[HiveLateralViewJoinOperator] with NaryOperator[HiveLateralViewJoinOperator] {
  override def execute: RDD[_] = {
    // Execute LateralViewForwardOperator, bypassing Select / UDTF - Select
    // branches (see diagram in Hive's).
    val conf = parentOperators.filter(_.isInstanceOf[UDTFOperator]).head
      .parentOperators.head.asInstanceOf[SelectOperator].hiveOp.getConf()
    val udtfOp = parentOperators.filter(_.isInstanceOf[UDTFOperator]).head.asInstanceOf[UDTFOperator]
    val lvfOp = parentOperators.filter(_.isInstanceOf[SelectOperator]).head.parentOperators.head
      .asInstanceOf[LateralViewForwardOperator]
    val partitionProcessor = new LateralViewJoinOperator.LateralViewJoinPartitionProcessor(
        conf,
        new SerializableObjectInspectors(objectInspectors.toArray),
        udtfOp.makePartitionProcessor(),
        new SerializableObjectInspectors(lvfOp.objectInspectors.toArray))
    
    val inputRDD = lvfOp.execute()
    PartitionProcessor.executeProcessPartition(partitionProcessor, inputRDD, this.toString(), this.objectInspectors.toString())
  }
}

object LateralViewJoinOperator {
  private class LateralViewJoinPartitionProcessor(
      private val conf: SelectDesc,
      private val objectInspectors: SerializableObjectInspectors[ObjectInspector],
      private val udtfPartitionProcessor: UDTFOperator.UDTFPartitionProcessor,
      private val lvfObjectInspectors: SerializableObjectInspectors[ObjectInspector])
      extends PartitionProcessor {
    
    /** Per existing row, emit a new row with each value of the exploded array */
    override def processPartition(split: Int, iter: Iterator[_]) = {
      
      // Get eval(), which will return array that needs to be exploded
      // eval doesn't exist when getColList() is null, but this happens only on select *'s,
      // which are not allowed within explode
      val eval = conf.getColList().map(ExprNodeEvaluatorFactory.get(_)).toArray
      eval.foreach(_.initialize(objectInspectors.value.head))
      
      val lvfSoi = lvfObjectInspectors.value.head.asInstanceOf[StructObjectInspector]
      val lvfFields = lvfSoi.getAllStructFieldRefs()
      
      //HACK: This should be refactored in some way to avoid the dependency on
      // an implementation detail of UDTFOperator.
      val explodeMethod = udtfPartitionProcessor.makeExploder()
      
      iter.flatMap { row =>
        var arrToExplode = eval.map(x => x.evaluate(row))
        val explodedRows = explodeMethod(arrToExplode)
  
        explodedRows.map { expRow =>
          val expRowArray = expRow.asInstanceOf[Array[java.lang.Object]]
          val joinedRow = new Array[java.lang.Object](lvfFields.size + expRowArray.length)
  
          // Add row fields from LateralViewForward
          var i = 0
          while (i < lvfFields.size) {
            joinedRow(i) = lvfSoi.getStructFieldData(row, lvfFields.get(i))
            i += 1
          }
          // Append element(s) from explode
          i = 0
          while (i < expRowArray.length) {
            joinedRow(i + lvfFields.size) = expRowArray(i)
            i += 1
          }
          joinedRow
        }
      }
    }
  }
}