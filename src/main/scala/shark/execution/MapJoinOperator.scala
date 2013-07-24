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

import java.util.{ArrayList, HashMap => JHashMap, List => JList}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty
import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, JoinUtil => HiveJoinUtil}
import org.apache.hadoop.hive.ql.exec.{MapJoinOperator => HiveMapJoinOperator}
import org.apache.hadoop.hive.ql.plan.MapJoinDesc
import org.apache.hadoop.hive.ql.plan.{PartitionDesc, TableDesc}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.BytesWritable
import shark.SharkEnv
import shark.SharkEnvSlave
import shark.execution.serialization.{SerializableWritable}
import spark.RDD
import spark.broadcast.Broadcast
import shark.execution.serialization.SerializableHiveConf
import shark.execution.serialization.SerializableObjectInspectors
import shark.execution.serialization.XmlSerializer


/**
 * A join operator optimized for joining a large table with a number of small
 * tables that fit in memory. The join can be performed as a map only job that
 * avoids an expensive shuffle process.
 *
 * Different from Hive, we don't spill the hash tables to disk. If the "small"
 * tables are too big to fit in memory, the normal join should be used anyway.
 */
class MapJoinOperator extends Operator[HiveMapJoinOperator] with NaryOperator[HiveMapJoinOperator] {
  @transient lazy val commonJoinState = new CommonJoinPartitionProcessingState(
      hiveOp.getConf(),
      new SerializableHiveConf(hconf, XmlSerializer.getUseCompression(hconf)),
      new SerializableObjectInspectors(objectInspectors.toArray),
      parentOperators.size)
  @transient private lazy val posBigTable: Int = hiveOp.getConf.getPosBigTable()
  @transient private lazy val bigTableAlias: Int = commonJoinState.order(posBigTable).toInt
  @transient private lazy val bigTableAliasByte: java.lang.Byte = bigTableAlias.toByte
  
  //NOTE: Both of these are initialized together, hence the awkward joint
  // declaration and initialization.
  @transient private lazy val (joinKeys, joinKeysObjectInspectors): (JHashMap[java.lang.Byte, JList[ExprNodeEvaluator]], JHashMap[java.lang.Byte, JList[ObjectInspector]]) = {
    val tmpJoinKeys = new JHashMap[java.lang.Byte, JList[ExprNodeEvaluator]]
    HiveJoinUtil.populateJoinKeyValue(
      tmpJoinKeys, commonJoinState.conf.getKeys(), commonJoinState.order, CommonJoinOperator.NOTSKIPBIGTABLE)

    // A bit confusing but getObjectInspectorsFromEvaluators also initializes
    // the evaluators.
    val tmpJoinKeysObjectInspectors = HiveJoinUtil.getObjectInspectorsFromEvaluators(
      tmpJoinKeys, objectInspectors.toArray, CommonJoinOperator.NOTSKIPBIGTABLE)
    (tmpJoinKeys, tmpJoinKeysObjectInspectors)
  }

  override def execute(): RDD[_] = {
    val inputRdds = executeParents()
    combineMultipleRdds(inputRdds)
  }

  override def executeParents(): Seq[(Int, RDD[_])] = {
    commonJoinState.order.zip(parentOperators).map(x => (x._1.toInt, x._2.execute))
  }

  private def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    logInfo("%d small tables to map join a large table (%d)".format(rdds.size - 1, posBigTable))
    logInfo("Big table alias " + bigTableAlias)

    // Build hash tables for the small tables.
    val hashtables = rdds.zipWithIndex.filter(_._2 != bigTableAlias).map { case ((_, rdd), pos) =>

      logInfo("Creating hash table for input %d".format(pos))

      // First compute the keys and values of the small RDDs on slaves.
      // We need to do this before collecting the RDD because the RDD might
      // contain lazy structs that cannot be properly collected directly.
      val posByte = pos.toByte

      val joinKeyValuesProcessor = new MapJoinOperator.JoinKeyValueComputationProcessor(
          commonJoinState,
          new SerializableObjectInspectors(objectInspectors),
          joinKeys,
          joinKeysObjectInspectors,
          posByte)
      
      val rddForHash: RDD[(Seq[AnyRef], Seq[Array[AnyRef]])] =
        rdd.mapPartitionsWithIndex({(split: Int, partition: Iterator[_]) =>
          // Put serialization metadata for values in slave's MapJoinMetaData.
          // Needed to serialize values in collect().
          //op.setValueMetaData(posByte)
          joinKeyValuesProcessor
            .processPartition(split, partition)
            .asInstanceOf[Iterator[(Seq[AnyRef], Seq[Array[AnyRef]])]]
        })

      // Collect the RDD and build a hash table.
      val startCollect = System.currentTimeMillis()
      val wrappedRows: Array[(Seq[AnyRef], Seq[Array[AnyRef]])] = rddForHash.collect()
      val collectTime = System.currentTimeMillis() - startCollect
      logInfo("HashTable collect took " + collectTime + " ms")

      // Build the hash table.
      val hash = wrappedRows.groupBy(x => x._1)
       .mapValues(v => v.flatMap(t => t._2))

      val map = new JHashMap[Seq[AnyRef], Array[Array[AnyRef]]]()
      hash.foreach(x => map.put(x._1, x._2))
      (pos, map)
    }.toMap

    val fetcher = SharkEnv.sc.broadcast(hashtables)
    val joinProcessor = new MapJoinOperator.JoinProcessor(commonJoinState, fetcher, joinKeys, joinKeysObjectInspectors)
    rdds(bigTableAlias)._2.mapPartitionsWithIndex(joinProcessor.processPartition _)
  }
}

object MapJoinOperator {
  private class JoinKeyValueComputationProcessor(
      private val commonJoinState: CommonJoinPartitionProcessingState[MapJoinDesc],
      private val objectInspectors: SerializableObjectInspectors[ObjectInspector],
      private val joinKeys: JHashMap[java.lang.Byte, JList[ExprNodeEvaluator]],
      private val joinKeysObjectInspectors: JHashMap[java.lang.Byte, JList[ObjectInspector]],
      private val posByte: Byte)
      extends PartitionProcessor {
    def processPartition(split: Int, iter: Iterator[_]):
        Iterator[(Seq[AnyRef], Seq[Array[AnyRef]])] = {
      // MapJoinObjectValue contains a MapJoinRowContainer, which contains a list of
      // rows to be joined.
      var valueMap = new JHashMap[Seq[AnyRef], Seq[Array[AnyRef]]]
      iter.foreach { row =>
        val key = JoinUtil.computeJoinKey(
          row,
          joinKeys.get(posByte),
          joinKeysObjectInspectors.get(posByte))
        val value: Array[AnyRef] = JoinUtil.computeJoinValues(
          row,
          commonJoinState.joinVals.get(posByte),
          commonJoinState.joinValuesObjectInspectors.get(posByte),
          commonJoinState.joinFilters.get(posByte),
          commonJoinState.joinFilterObjectInspectors.get(posByte),
          commonJoinState.noOuterJoin)
        // If we've seen the key before, just add it to the row container wrapped by
        // corresponding MapJoinObjectValue.
        val objValue = valueMap.get(key)
        if (objValue == null) {
          valueMap.put(key, Seq[Array[AnyRef]](value))
        } else {
          valueMap.put(key, objValue ++ List[Array[AnyRef]](value))
        }
      }
      valueMap.iterator
    }
  }
  
  private class JoinProcessor(
      private val commonJoinState: CommonJoinPartitionProcessingState[MapJoinDesc],
      private val fetcher: spark.broadcast.Broadcast[Map[Int,java.util.HashMap[Seq[AnyRef],Array[Array[AnyRef]]]]],
      private val joinKeys: JHashMap[java.lang.Byte, JList[ExprNodeEvaluator]],
      private val joinKeysObjectInspectors: JHashMap[java.lang.Byte, JList[ObjectInspector]])
      extends PartitionProcessor {
    def processPartition(split: Int, partition: Iterator[_]): Iterator[_] = {
      logDebug("Started executing mapPartitions for MapJoinOperator")
//      logDebug("Input object inspectors: " + op.objectInspectors) //FIXME

      val newPart = joinOnPartition(partition, fetcher.value)
      logDebug("Finished executing mapPartitions for MapJoinOperator")

      newPart
    }
    /**
     * Stream through the large table and process the join using the hash tables.
     * Note that this is a specialized processPartition that accepts an extra
     * parameter for the hash tables (built from the small tables).
     */
    def joinOnPartition[T](
        iter: Iterator[T],
        hashtables: Map[Int, JHashMap[Seq[AnyRef], Array[Array[AnyRef]]]]): Iterator[_] = {
      val posBigTable: Int = commonJoinState.conf.getPosBigTable()
      val bigTableAlias: Int = commonJoinState.order(posBigTable).toInt
      val bigTableAliasByte: java.lang.Byte = bigTableAlias.toByte
      val joinKeyEval = joinKeys.get(bigTableAlias.toByte)
      val joinValueEval = commonJoinState.joinVals.get(bigTableAlias.toByte)
      val bufs = new Array[Seq[Array[Object]]](commonJoinState.numTables)
      val nullSafes = commonJoinState.conf.getNullSafes()
  
      val cp = new CartesianProduct[Array[Object]](commonJoinState.numTables)
  
      val jointRows: Iterator[Array[Array[Object]]] = iter.flatMap { row =>
        // Build the join key and value for the row in the large table.
        val key = JoinUtil.computeJoinKey(
          row,
          joinKeyEval,
          joinKeysObjectInspectors.get(bigTableAliasByte))
        val v: Array[AnyRef] = JoinUtil.computeJoinValues(
          row,
          joinValueEval,
          commonJoinState.joinValuesObjectInspectors.get(bigTableAliasByte),
          commonJoinState.joinFilters.get(bigTableAliasByte),
          commonJoinState.joinFilterObjectInspectors.get(bigTableAliasByte),
          commonJoinState.noOuterJoin)
        val value = new Array[AnyRef](v.size)
        Range(0,v.size).foreach(i => value(i) = v(i).asInstanceOf[SerializableWritable[_]].value.asInstanceOf[AnyRef])
  
        if (commonJoinState.nullCheck && JoinUtil.joinKeyHasAnyNulls(key, nullSafes)) {
          val bufsNull = Array.fill[Seq[Array[Object]]](commonJoinState.numTables)(Seq())
          bufsNull(bigTableAlias) = Seq(value)
          cp.product(bufsNull, commonJoinState.joinConditions)
        } else {
          // Build the join bufs.
          var i = 0
          while ( i < commonJoinState.numTables) {
            if (i == bigTableAlias) {
              bufs(i) = Seq[Array[AnyRef]](value)
            } else {
              val smallTableValues = hashtables.getOrElse(i, null).getOrElse(key, null)
              bufs(i) =
                if (smallTableValues == null) {
                  Seq.empty[Array[AnyRef]]
                } else {
                  smallTableValues.map { x =>
                    x.map(v => v.asInstanceOf[SerializableWritable[_]].value.asInstanceOf[AnyRef])
                  }
                }
            }
            i += 1
          }
          cp.product(bufs, commonJoinState.joinConditions)
        }
      }
      val rowSize = commonJoinState.joinVals.values.map(_.size).sum
      val rowToReturn = new Array[Object](rowSize)
      // For each row, combine the tuples from multiple tables into a single tuple.
      jointRows.map { row: Array[Array[Object]] =>
        var tupleIndex = 0
        var fieldIndex = 0
        row.foreach { tuple =>
          val stop = fieldIndex + commonJoinState.joinVals(tupleIndex.toByte).size
          var fieldInTuple = 0
          if (tuple == null) {
            // For outer joins, it is possible to see nulls.
            while (fieldIndex < stop) {
              rowToReturn(fieldIndex) = null
              fieldInTuple += 1
              fieldIndex += 1
            }
          } else {
            while (fieldIndex < stop) {
              rowToReturn(fieldIndex) = tuple.asInstanceOf[Array[Object]](fieldInTuple)
              fieldInTuple += 1
              fieldIndex += 1
            }
          }
          tupleIndex += 1
        }
  
        rowToReturn
      }
    }
  }
}
