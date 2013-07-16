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

import java.util.{ArrayList, List => JavaList}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty
import org.apache.hadoop.hive.ql.exec.{UnionOperator => HiveUnionOperator}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import shark.SharkEnvSlave
import spark.RDD
import spark.rdd.UnionRDD
import shark.execution.serialization.SerializableObjectInspectors


/**
 * A union operator. If the incoming data are of different type, the union
 * operator transforms the incoming data into the same type.
 */
class UnionOperator extends Operator[HiveUnionOperator] with NaryOperator[HiveUnionOperator] {
  import UnionOperator._

  override def execute(): RDD[_] = {
    val inputRdds = executeParents()
    combineMultipleRdds(inputRdds)
  }

  private def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    val rddsInOrder: Seq[RDD[Any]] = rdds.sortBy(_._1).map(_._2.asInstanceOf[RDD[Any]])
    val needsTransform = makeNeedsTransform()
          
    val rddsTransformed = rddsInOrder.zipWithIndex.map { case(rdd, tag) =>
      if (needsTransform(tag)) {
        transformRdd(rdd, tag)
      } else {
        rdd
      }
    }

    new UnionRDD(rddsTransformed.head.context, rddsTransformed.asInstanceOf[Seq[RDD[Any]]])
  }
  
  // whether we need to do transformation for each parent
  // We reuse needsTransform from Hive because the comparison of object
  // inspectors are hard once we send object inspectors over the wire.
  private def makeNeedsTransform(): Array[Boolean] = {
    val parentFields = getParentFields(getParentObjectInspectors(objectInspectors))
    val numColumns = parentFields.head.size()
    val numParents = parentOperators.size
    val columnTypeResolvers = makeColumnTypeResolvers(numColumns, numParents, parentFields)
    val columnNames = parentFields.head.map(_.getFieldName())
    val outputFieldOIs = columnTypeResolvers.map(_.get())
    val outputObjInspector = SharkEnvSlave.objectInspectorLock.synchronized {
      ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, outputFieldOIs.toList)
    }
    val needsTransformField = hiveOp.getClass.getDeclaredField("needsTransform")
    needsTransformField.setAccessible(true)
    // whether we need to do transformation for each parent
    // We reuse needsTransform from Hive because the comparison of object
    // inspectors are hard once we send object inspectors over the wire.
    val needsTransform = needsTransformField.get(hiveOp).asInstanceOf[Array[Boolean]]
    needsTransform.zipWithIndex.filter(_._1).foreach { case(transform, p) =>
      logInfo("Union Operator needs to transform row from parent[%d] from %s to %s".format(
          p, objectInspectors(p), outputObjInspector))
    }
    needsTransform
  }

  private def transformRdd(rdd: RDD[_], tag: Int) = {
    val serializableObjectInspectors = new SerializableObjectInspectors(objectInspectors.toArray)
    val numParents = parentOperators.size
    val partitionProcessor = new UnionOperator.UnionPartitionProcessor(tag, numParents, serializableObjectInspectors)
    rdd.mapPartitionsWithIndex(partitionProcessor.processPartition _)
  }
}

object UnionOperator {
  private def makeColumnTypeResolvers(
      numColumns: Int,
      numParents: Int,
      parentFields: Seq[java.util.List[_ <: StructField]]):
      Seq[ReturnObjectInspectorResolver] = {
    val columnTypeResolvers = Array.fill(numColumns)(new ReturnObjectInspectorResolver(true))

    for (p <- 0 until numParents) {
      assert(parentFields(p).size() == numColumns)
      for (c <- 0 until numColumns) {
        columnTypeResolvers(c).update(parentFields(p).get(c).getFieldObjectInspector())
      }
    }
    columnTypeResolvers
  }
  
  private def getParentFields(parentObjectInspectors: Seq[StructObjectInspector]): Seq[java.util.List[_ <: StructField]] = {
    parentObjectInspectors.map(_.getAllStructFieldRefs())
  }
  
  private def getParentObjectInspectors(objectInspectors: Seq[ObjectInspector]): Seq[StructObjectInspector] = {
    objectInspectors.filter(_ != null).map(_.asInstanceOf[StructObjectInspector])
  }
  
  class UnionPartitionProcessor(
      private val tag: Int,
      private val numParents: Int,
      private val objectInspectors: SerializableObjectInspectors[ObjectInspector])
      extends PartitionProcessor {
    override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = {
      val parentObjInspectors = getParentObjectInspectors(objectInspectors.value)
      val parentFields = parentObjInspectors.map(_.getAllStructFieldRefs())

      val numColumns = parentFields.head.size()
      
      val columnTypeResolvers = makeColumnTypeResolvers(numColumns, numParents, parentFields)
      
      val outputRow = new ArrayList[Object](numColumns)
      for (c <- 0 until numColumns) outputRow.add(null)

      iter.map { row =>
        val soi = parentObjInspectors(tag)
        val fields = parentFields(tag)

        for (c <- 0 until fields.size) {
          outputRow.set(c, columnTypeResolvers(c).convertIfNecessary(soi
              .getStructFieldData(row, fields.get(c)), fields.get(c)
              .getFieldObjectInspector()))
        }

        outputRow
      }
    }
  }
}

