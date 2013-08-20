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

import scala.collection.Iterator
import scala.reflect.BeanProperty
import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.exec.{FilterOperator => HiveFilterOperator}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.plan.FilterDesc
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import shark.execution.serialization.SerializableObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import shark.execution.serialization.SerializableOperatorDescriptor
import shark.execution.serialization.XmlSerializer


class FilterOperator extends SimpleUnaryOperator[HiveFilterOperator] {
  //HACK: TableScanOperator currently requires this.  This is horrible.
  def getExprNodeEvaluator(): ExprNodeEvaluator = {
    ExprNodeEvaluatorFactory.get(hiveOp.getConf.getPredicate())
  }
  
  override def makePartitionProcessor(): PartitionProcessor = {
    new FilterOperator.FilterPartitionProcessor(
        new SerializableOperatorDescriptor(hiveOp.getConf(), XmlSerializer.getUseCompression(hconf)),
        new SerializableObjectInspector(objectInspector))
  }
}

object FilterOperator {
  private class FilterPartitionProcessor(
      private val conf: SerializableOperatorDescriptor[FilterDesc],
      private val objectInspector: SerializableObjectInspector[ObjectInspector])
      extends PartitionProcessor {
    override def processPartition(split: Int, iter: Iterator[_]) = {
      val (conditionEvaluator, conditionInspector) = try {
        val conditionEvaluator = ExprNodeEvaluatorFactory.get(conf.value.getPredicate())
        val conditionInspector = conditionEvaluator.initialize(objectInspector.value)
          .asInstanceOf[PrimitiveObjectInspector]
        (conditionEvaluator, conditionInspector)
      } catch {
        case e: Throwable => throw new HiveException(e)
      }
      
      iter.filter { row =>
        java.lang.Boolean.TRUE.equals(
          conditionInspector.getPrimitiveJavaObject(conditionEvaluator.evaluate(row)))
      }
    }
  }
}