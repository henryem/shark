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

import java.util.{HashMap => JHashMap, List => JList}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{JoinOperator => HiveJoinOperator}
import org.apache.hadoop.hive.ql.plan.{JoinDesc, TableDesc}
import org.apache.hadoop.hive.serde2.{Deserializer, SerDeUtils}
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector
import org.apache.hadoop.io.BytesWritable
import shark.execution.serialization.SerializableObjectInspectors
import spark.{CoGroupedRDD, HashPartitioner, RDD}
import shark.execution.serialization.SerializableHiveConf
import shark.execution.serialization.XmlSerializer
import shark.execution.serialization.SerializableObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector


class JoinOperator extends Operator[HiveJoinOperator] with NaryOperator[HiveJoinOperator]
  with HiveTopOperator[HiveJoinOperator] {
  private val inputObjectInspectors = new scala.collection.mutable.HashMap[Int, SerializableObjectInspector[ObjectInspector]]
  private val keyValueTableDescs = new scala.collection.mutable.HashMap[Int, (TableDesc, TableDesc)]
  
  override def initializeHiveTopOperator() {
    HiveTopOperator.initializeHiveTopOperator(this)
  }

  override def setInputObjectInspector(tag: Int, objectInspector: ObjectInspector) {
    inputObjectInspectors.put(tag, new SerializableObjectInspector(objectInspector))
  }

  override def getInputObjectInspectors(): Map[Int, ObjectInspector] = {
    inputObjectInspectors.mapValues(_.value).toMap
  }
  
  override def setKeyValueTableDescs(tag: Int, descs: (TableDesc, TableDesc)) {
    keyValueTableDescs.put(tag, descs)
  }
  
  override def execute(): RDD[_] = {
    val inputRdds = executeParents()
    combineMultipleRdds(inputRdds)
  }

  private def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    // Determine the number of reduce tasks to run.
    val numReduceTasks = math.max(1, hconf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS))
    
    val commonJoinState = new CommonJoinPartitionProcessingState(
        hiveOp.getConf(),
        new SerializableHiveConf(hconf, XmlSerializer.getUseCompression(hconf)),
        new SerializableObjectInspectors(objectInspectors.toArray),
        parentOperators.size)

    // Turn the RDD into a map. Use a Java HashMap to avoid Scala's annoying
    // Some/Option. Add an assert for sanity check. If ReduceSink's join tags
    // are wrong, the hash entries might collide.
    val rddsJavaMap = new JHashMap[Int, RDD[_]]
    rddsJavaMap ++= rdds
    assert(rdds.size == rddsJavaMap.size, {
      logError("rdds.size (%d) != rddsJavaMap.size (%d)".format(rdds.size, rddsJavaMap.size))
    })

    val rddsInJoinOrder = commonJoinState.order.map { inputIndex =>
      rddsJavaMap.get(inputIndex.byteValue.toInt).asInstanceOf[RDD[(ReduceKey, Any)]]
    }

    val part = new HashPartitioner(numReduceTasks)
    val cogrouped = new CoGroupedRDD[ReduceKey](
      rddsInJoinOrder.toSeq.asInstanceOf[Seq[RDD[(_, _)]]], part)
    
      
    val valueTableDescMap = new JHashMap[Int, TableDesc]
    valueTableDescMap ++= keyValueTableDescs.map { case(tag, kvdescs) => (tag, kvdescs._2) }
    val keyTableDesc = keyValueTableDescs.head._2._1
    val processor = new JoinOperator.JoinPartitionProcessor(commonJoinState, valueTableDescMap, keyTableDesc)
    PartitionProcessor.executeProcessPartition(processor, cogrouped, this.toString(), objectInspectors.toString())
  }
}

object JoinOperator {
  private class JoinPartitionProcessor(
      private val commonJoinState: CommonJoinPartitionProcessingState[JoinDesc],
      private val valueTableDescMap: JHashMap[Int, TableDesc],
      private val keyTableDesc: TableDesc)
      extends PartitionProcessor {
    @transient private lazy val tagToValueSer = {
      val tagToValueSer = new JHashMap[Int, Deserializer]
      valueTableDescMap foreach { case(tag, tableDesc) =>
        logDebug("tableDescs (tag %d): %s".format(tag, tableDesc))
  
        val deserializer = tableDesc.getDeserializerClass.newInstance()
        deserializer.initialize(null, tableDesc.getProperties())
  
        logDebug("value deser (tag %d): %s".format(tag, deserializer))
        tagToValueSer.put(tag, deserializer)
      }
      tagToValueSer
    }
    
    override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = {
      val tmp = new Array[Object](2)
      val writable = new BytesWritable
      val nullSafes = commonJoinState.conf.getNullSafes()

      val cp = new CartesianProduct[Any](commonJoinState.numTables)

      iter.flatMap { case (k: ReduceKeyReduceSide, bufs: Array[_]) =>
        writable.set(k.byteArray, 0, k.length)

        // If nullCheck is false, we can skip deserializing the key.
        if (commonJoinState.nullCheck) {
          val keyDeserializer = keyTableDesc.getDeserializerClass.newInstance().asInstanceOf[Deserializer]
          keyDeserializer.initialize(null, keyTableDesc.getProperties())
          val keyObjectInspector =
            keyDeserializer.getObjectInspector().asInstanceOf[StandardStructObjectInspector]
          if (SerDeUtils.hasAnyNullObject(
              keyDeserializer.deserialize(writable).asInstanceOf[JList[_]],
              keyObjectInspector,
              nullSafes)) {
            //FIXME: Not sure what type @buf will have.  Hopefully it is a Seq.
            bufs.zipWithIndex.flatMap { case (buf: Seq[_], label) =>
              val bufsNull: Array[Seq[Any]] = Array.fill(commonJoinState.numTables)(ArrayBuffer[Any]())
              bufsNull(label) = buf
              generateTuples(cp.product(bufsNull.asInstanceOf[Array[Seq[Any]]], commonJoinState.joinConditions))
            }
          } else {
            generateTuples(cp.product(bufs.asInstanceOf[Array[Seq[Any]]], commonJoinState.joinConditions))
          }
        } else {
          generateTuples(cp.product(bufs.asInstanceOf[Array[Seq[Any]]], commonJoinState.joinConditions))
        }
      }
    }
    
    private def generateTuples(iter: Iterator[Array[Any]]): Iterator[_] = {
      //val tupleOrder = CommonJoinOperator.computeTupleOrder(joinConditions)
  
      // TODO: use MutableBytesWritable to avoid the array copy.
      val bytes = new BytesWritable
      val tmp = new Array[Object](2)
  
      val tupleSizes = (0 until commonJoinState.joinVals.size).map { i => commonJoinState.joinVals.get(i.toByte).size() }.toIndexedSeq
      val offsets = tupleSizes.scanLeft(0)(_ + _)
  
      val rowSize = offsets.last
      val outputRow = new Array[Object](rowSize)
  
      iter.map { elements: Array[Any] =>
        var index = 0
        while (index < commonJoinState.numTables) {
          val element = elements(index).asInstanceOf[Array[Byte]]
          var i = 0
          if (element == null) {
            while (i < commonJoinState.joinVals.get(index.toByte).size) {
              outputRow(i + offsets(index)) = null
              i += 1
            }
          } else {
            bytes.set(element, 0, element.length)
            tmp(1) = tagToValueSer.get(index).deserialize(bytes)
            val joinVal = commonJoinState.joinVals.get(index.toByte)
            while (i < joinVal.size) {
              outputRow(i + offsets(index)) = joinVal(i).evaluate(tmp)
              i += 1
            }
          }
          index += 1
        }
  
        outputRow
      }
    }
  }
}
