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

package org.apache.hadoop.hive.ql.exec

// Put this file in Hive's exec package to access package level visible fields and methods.

import java.lang.{Integer => JInteger}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, HashSet => JHashSet, Set => JSet, Map => JMap}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{GroupByOperator => HiveGroupByOperator}
import org.apache.hadoop.hive.ql.plan.{ExprNodeColumnDesc, TableDesc}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorUtils,
  StandardStructObjectInspector, StructObjectInspector, UnionObject}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.io.BytesWritable
import shark.SharkEnv
import shark.execution._
import shark.execution.{GroupByOperator => SharkGroupByOperator}
import spark.{Aggregator, HashPartitioner, RDD}
import spark.rdd.ShuffledRDD
import shark.execution.serialization.SerializableObjectInspector
import shark.SharkEnvSlave
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.ql.plan.AggregationDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.GroupByDesc
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator
import shark.execution.GroupByOperator.CommonGroupByState


// The final phase of group by.
// TODO(rxin): For multiple distinct aggregations, use sort-based shuffle.
class GroupByPostShuffleOperator extends UnaryOperator[HiveGroupByOperator]
    with HiveTopOperator[HiveGroupByOperator] {
  @transient private lazy val conf: GroupByDesc = hiveOp.getConf()
  @transient private lazy val minReductionHashAggr: Float = hconf.get(HiveConf.ConfVars.HIVEMAPAGGRHASHMINREDUCTION.varname).toFloat
  @transient private lazy val numRowsCompareHashAggr: Int = hconf.get(HiveConf.ConfVars.HIVEGROUPBYMAPINTERVAL.varname).toInt
  
  @transient private lazy val commonState = CommonGroupByState(conf, new SerializableObjectInspector(objectInspectors.head.asInstanceOf[StructObjectInspector]))
  
  private val inputObjectInspectors = new scala.collection.mutable.HashMap[Int, SerializableObjectInspector[ObjectInspector]]
  private val keyValueTableDescs = new scala.collection.mutable.HashMap[Int, (TableDesc, TableDesc)]
  
  @transient private lazy val keyTableDesc: TableDesc = keyValueTableDescs.values.head._1
  @transient private lazy val valueTableDesc: TableDesc = keyValueTableDescs.values.head._2
  
  override def execute(): RDD[_] = {
    val inputRdd = executeParents().head._2.asInstanceOf[RDD[(Any, Any)]]

    var numReduceTasks = hconf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS)
    // If we have no keys, it needs a total aggregation with 1 reducer.
    if (numReduceTasks < 1 || conf.getKeys.size == 0) numReduceTasks = 1
    val partitioner = new HashPartitioner(numReduceTasks)

    val repartitionedRDD = new ShuffledRDD[Any, Any](
      inputRdd, partitioner, SharkEnv.shuffleSerializerName)

    val (distinctKeyAggrs, nonDistinctKeyAggrs) = GroupByPostShuffleOperator.makeKeyAggrs(conf, commonState.unionExprEvaluator)
    if (distinctKeyAggrs.size > 0) {
      // If there are distinct aggregations, do sort-based aggregation.
      val sortAggregator = new GroupByPostShuffleOperator.SortAggregationProcessor(commonState, keyTableDesc, valueTableDesc)
      repartitionedRDD.mapPartitionsWithIndex(sortAggregator.processPartition _, preservesPartitioning = true)
    } else {
      // No distinct keys.
      val aggregator = new Aggregator[Any, Any, ArrayBuffer[Any]](
        GroupByAggregator.createCombiner _, GroupByAggregator.mergeValue _, null)
      val hashedRdd = repartitionedRDD.mapPartitions(aggregator.combineValuesByKey(_),
        preservesPartitioning = true)
      
      val hashAggregator = new GroupByPostShuffleOperator.HashAggregationProcessor(commonState, keyTableDesc, valueTableDesc)
      hashedRdd.mapPartitionsWithIndex(hashAggregator.processPartition _)
    }
  }
  
  override def initializeHiveTopOperator() {
    HiveTopOperator.initializeHiveTopOperator(this, inputObjectInspectors.mapValues(_.value).toMap)
  }
  
  override def setInputObjectInspector(tag: Int, objectInspector: ObjectInspector) {
    inputObjectInspectors.put(tag, objectInspector)
  }
  
  override def setKeyValueTableDescs(tag: Int, descs: (TableDesc, TableDesc)) {
    keyValueTableDescs.put(tag, descs)
  }
}

object GroupByPostShuffleOperator {
  private def makeKeySer(keyTableDesc: TableDesc): Deserializer = {
    val keySerTmp = keyTableDesc.getDeserializerClass.newInstance()
    keySerTmp.initialize(null, keyTableDesc.getProperties())
    keySerTmp
  }
  
  private def makeValueSer(valueTableDesc: TableDesc): Deserializer = {
    val valueSerTmp = valueTableDesc.getDeserializerClass.newInstance()
    valueSerTmp.initialize(null, valueTableDesc.getProperties())
    valueSerTmp
  }
  
  
  private def makeKeyAggrs(conf: GroupByDesc, unionExprEvaluator: ExprNodeEvaluator): (JMap[Int, JSet[JInteger]], JMap[Int, JSet[JInteger]]) = {
    val tmpDistinctKeyAggrs = new JHashMap[Int, JSet[java.lang.Integer]]()
    val tmpNonDistinctKeyAggrs = new JHashMap[Int, JSet[java.lang.Integer]]()
    val aggrs = conf.getAggregators
    for (i <- 0 until aggrs.size) {
      val aggr = aggrs.get(i)
      val parameters = aggr.getParameters
      for (j <- 0 until parameters.size) {
        if (unionExprEvaluator != null) {
          val names = parameters.get(j).getExprString().split("\\.")
          // parameters of the form : KEY.colx:t.coly
          if (Utilities.ReduceField.KEY.name().equals(names(0))) {
            val name = names(names.length - 2)
            val tag = Integer.parseInt(name.split("\\:")(1))
            if (aggr.getDistinct()) {
              var set = tmpDistinctKeyAggrs.get(tag)
              if (set == null) {
                set = new JHashSet[java.lang.Integer]()
                tmpDistinctKeyAggrs.put(tag, set)
              }
              if (!set.contains(i)) {
                set.add(i)
              }
            } else {
              var set = tmpNonDistinctKeyAggrs.get(tag)
              if (set == null) {
                set = new JHashSet[java.lang.Integer]()
                tmpNonDistinctKeyAggrs.put(tag, set)
              }
              if (!set.contains(i)) {
                set.add(i)
              }
            }
          }
        }
      }
    }
    (tmpDistinctKeyAggrs, tmpNonDistinctKeyAggrs)
  }
  
  private class HashAggregationProcessor(
      private val commonState: CommonGroupByState,
      private val keyTableDesc: TableDesc,
      private val valueTableDesc: TableDesc)
      extends PartitionProcessor {
    @transient private lazy val keySer: Deserializer = makeKeySer(keyTableDesc)
    @transient private lazy val valueSer: Deserializer = makeValueSer(valueTableDesc)
    
    override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = {
      // TODO: use MutableBytesWritable to avoid the array copy.
      val bytes = new BytesWritable()
      logInfo("Running Post Shuffle Group-By")
      val outputCache = new Array[Object](commonState.keyFields.length + commonState.aggregationEvals.length)
  
      // The reusedRow is used to conform to Hive's expected row format.
      // It is an array of [key, value] that is reused across rows
      val reusedRow = new Array[Any](2)
      val aggrs = SharkGroupByOperator.newAggregations(commonState.aggregationEvals)
  
      val newIter = iter.map { case (key: ReduceKeyReduceSide, values: Seq[Array[Byte]]) =>
        bytes.set(key.byteArray, 0, key.length)
        val deserializedKey = keySer.deserialize(bytes)
        reusedRow(0) = deserializedKey
        SharkGroupByOperator.resetAggregations(aggrs, commonState.aggregationEvals)
        values.foreach { case v: Array[Byte] =>
          bytes.set(v, 0, v.length)
          reusedRow(1) = valueSer.deserialize(bytes)
          SharkGroupByOperator.aggregateExistingKey(reusedRow, aggrs, commonState.aggregationIsDistinct, commonState.aggregationEvals, commonState.aggregationParameterFields)
        }
  
        // Copy output keys and values to our reused output cache
        var i = 0
        val numKeys = commonState.keyFields.length
        while (i < numKeys) {
          outputCache(i) = commonState.keyFields(i).evaluate(reusedRow)
          i += 1
        }
        while (i < numKeys + aggrs.length) {
          outputCache(i) = commonState.aggregationEvals(i - numKeys).evaluate(aggrs(i - numKeys))
          i += 1
        }
        outputCache
      }
  
      if (!newIter.hasNext && commonState.keyFields.length == 0) {
        Iterator(SharkGroupByOperator.createEmptyRow(commonState.aggregationEvals, commonState.aggregationParameterFields)) // We return null if there are no rows
      } else {
        newIter
      }
    }
  }
  
  //TODO: This code is really complicated and could use some cleaning.
  private class SortAggregationProcessor(
      private val commonState: CommonGroupByState,
      private val keyTableDesc: TableDesc,
      private val valueTableDesc: TableDesc)
      extends PartitionProcessor {
    // Use two sets of key deserializer and value deserializer because in sort-based aggregations,
    // we need to keep two rows deserialized at any given time (to compare whether we have seen
    // a new input group by key).
    @transient private lazy val keySer: Deserializer = makeKeySer(keyTableDesc)
    @transient private lazy val keySer1: Deserializer = makeKeySer(keyTableDesc)
    @transient private lazy val valueSer: Deserializer = makeValueSer(valueTableDesc)
    @transient private lazy val valueSer1: Deserializer = makeValueSer(valueTableDesc)
    
    @transient private lazy val (distinctKeyAggrs, nonDistinctKeyAggrs): (JMap[Int, JSet[JInteger]], JMap[Int, JSet[JInteger]]) = GroupByPostShuffleOperator.makeKeyAggrs(commonState.conf, commonState.unionExprEvaluator)
    
    override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = {
      // Sort the input based on the key.
      val buf = iter.toArray.asInstanceOf[Array[(ReduceKeyReduceSide, Array[Byte])]]
      val sorted = buf.sortWith((x, y) => x._1.compareTo(y._1) < 0).iterator

      // Perform sort-based aggregation.
      sortAggregate(sorted)
    }
    
    private def sortAggregate(iter: Iterator[_]) = {
      logInfo("Running Post Shuffle Group-By")
  
      if (iter.hasNext) {
        // Sort based aggregation iterator.
        new Iterator[Array[Object]]() {
  
          private var currentKeySer = keySer
          private var currentValueSer = valueSer
          private var nextKeySer = keySer1
          private var nextValueSer = valueSer1
  
          private val outputBuffer = new Array[Object](commonState.keyFields.length + commonState.aggregationEvals.length)
          private val bytes = new BytesWritable()
          private val row = new Array[Object](2)
          private val nextRow = new Array[Object](2)
          private val aggrs = SharkGroupByOperator.newAggregations(commonState.aggregationEvals)
  
          private val newKeys: KeyWrapper = commonState.keyFactory.getKeyWrapper()
  
          private var _hasNext: Boolean = fetchNextInputTuple()
          private val currentKeys: KeyWrapper = newKeys.copyKey()
  
          override def hasNext = _hasNext
  
          private def swapSerDes() {
            var tmp = currentKeySer
            currentKeySer = nextKeySer
            nextKeySer = tmp
            tmp = currentValueSer
            currentValueSer = nextValueSer
            nextValueSer = tmp
          }
  
          /**
           * Fetch the next input tuple and deserialize them, store the keys in newKeys.
           * @return True if successfully fetched; false if we have reached the end.
           */
          def fetchNextInputTuple(): Boolean = {
            if (!iter.hasNext) {
              false
            } else {
              val (key: ReduceKeyReduceSide, value: Array[Byte]) = iter.next()
              bytes.set(key.byteArray, 0, key.length)
              nextRow(0) = nextKeySer.deserialize(bytes)
              bytes.set(value, 0, value.length)
              nextRow(1) = nextValueSer.deserialize(bytes)
              newKeys.getNewKey(nextRow, commonState.rowInspector)
              swapSerDes()
              true
            }
          }
  
          override def next(): Array[Object] = {
  
            currentKeys.copyKey(newKeys)
            SharkGroupByOperator.resetAggregations(aggrs, commonState.aggregationEvals)
  
            // Use to track whether we have moved to a new distinct column.
            var currentUnionTag = -1
            // Use to track whether we have seen a new distinct value for the current distinct column.
            var lastDistinctKey: Array[Object] = null
  
            // Keep processing inputs until we see a new group by key.
            // In that case, we need to emit a new output tuple so the next() call should end.
            while (_hasNext && newKeys.equals(currentKeys)) {
              row(0) = nextRow(0)
              row(1) = nextRow(1)
  
              if (row(1) != null) {
                // Aggregate the non distinct columns from the values.
                SharkGroupByOperator.aggregateExistingKey(row, aggrs, commonState.aggregationIsDistinct, commonState.aggregationEvals, commonState.aggregationParameterFields)
              }
              
              // union tag is the tag (index) for the distinct column.
              val uo = commonState.unionExprEvaluator.evaluate(row).asInstanceOf[UnionObject]
              val unionTag = uo.getTag.toInt
  
              // update non-distinct aggregations : "KEY._colx:t._coly"
              val nonDistinctKeyAgg: JSet[java.lang.Integer] = nonDistinctKeyAggrs.get(unionTag)
              if (nonDistinctKeyAgg != null) {
                for (pos <- nonDistinctKeyAgg) {
                  val o = new Array[Object](commonState.aggregationParameterFields(pos).length)
                  var pi = 0
                  while (pi < commonState.aggregationParameterFields(pos).length) {
                    o(pi) = commonState.aggregationParameterFields(pos)(pi).evaluate(row)
                    pi += 1
                  }
                  commonState.aggregationEvals(pos).aggregate(aggrs(pos), o)
                }
              }
  
              // update distinct aggregations
              val distinctKeyAgg: JSet[java.lang.Integer] = distinctKeyAggrs.get(unionTag)
              if (distinctKeyAgg != null) {
                if (currentUnionTag != unionTag) {
                  // New union tag, i.e. move to a new distinct column.
                  currentUnionTag = unionTag
                  lastDistinctKey = null
                }
  
                val distinctKeyAggIter = distinctKeyAgg.iterator
                while (distinctKeyAggIter.hasNext) {
                  val pos = distinctKeyAggIter.next()
                  val o = new Array[Object](commonState.aggregationParameterFields(pos).length)
                  var pi = 0
                  while (pi < commonState.aggregationParameterFields(pos).length) {
                    o(pi) = commonState.aggregationParameterFields(pos)(pi).evaluate(row)
                    pi += 1
                  }
  
                  if (lastDistinctKey == null) {
                    lastDistinctKey = new Array[Object](o.length)
                  }
  
                  val isNewDistinct = ObjectInspectorUtils.compare(
                    o,
                    commonState.aggregationParameterObjectInspectors(pos),
                    lastDistinctKey,
                    commonState.aggregationParameterStandardObjectInspectors(pos)) != 0
  
                  if (isNewDistinct) {
                    // This is a new distinct value for the column.
                    commonState.aggregationEvals(pos).aggregate(aggrs(pos), o)
                    var pi = 0
                    while (pi < o.length) {
                      lastDistinctKey(pi) = ObjectInspectorUtils.copyToStandardObject(
                        o(pi), commonState.aggregationParameterObjectInspectors(pos)(pi),
                        ObjectInspectorCopyOption.WRITABLE)
                      pi += 1
                    }
                  }
                }
              }
  
              // Finished processing the current input tuple. Check if there are more inputs.
              _hasNext = fetchNextInputTuple()
            } // end of while (_hasNext && newKeys.equals(currentKeys))
  
            // Copy output keys and values to our reused output cache
            var i = 0
            val numKeys = commonState.keyFields.length
            while (i < numKeys) {
              outputBuffer(i) = commonState.keyFields(i).evaluate(row)
              i += 1
            }
            while (i < numKeys + aggrs.length) {
              outputBuffer(i) = commonState.aggregationEvals(i - numKeys).evaluate(aggrs(i - numKeys))
              i += 1
            }
            outputBuffer
          } // End of def next(): Array[Object]
        } // End of Iterator[Array[Object]]
      } else {
        // The input iterator is empty.
        if (commonState.keyFields.length == 0) {
          Iterator(SharkGroupByOperator.createEmptyRow(commonState.aggregationEvals, commonState.aggregationParameterFields))
        } else {
          Iterator.empty
        }
      }
    }
  }
}

object GroupByAggregator {
  def createCombiner(v: Any) = ArrayBuffer(v)
  def mergeValue(buf: ArrayBuffer[Any], v: Any) = buf += v
}
