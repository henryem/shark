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

import java.util.{ArrayList => JArrayList, HashMap => JHashMap}
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{GroupByOperator => HiveGroupByOperator}
import org.apache.hadoop.hive.ql.plan.{AggregationDesc, ExprNodeDesc, GroupByDesc}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory,
    ObjectInspectorUtils, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import shark.SharkEnvSlave
import shark.execution.UnaryOperator
import shark.execution.SimpleUnaryOperator
import shark.execution.PartitionProcessor
import shark.LogHelper
import shark.execution.serialization.SerializableObjectInspector
import shark.execution.serialization.SerializableHiveConf
import shark.execution.serialization.XmlSerializer


/**
 * The pre-shuffle group by operator responsible for map side aggregations.
 */
class GroupByPreShuffleOperator extends SimpleUnaryOperator[HiveGroupByOperator] {
  override def makePartitionProcessor(): PartitionProcessor = {
    new GroupByPreShuffleOperator.GroupByPreShufflePartitionProcessor(
        hiveOp.getConf(),
        new SerializableHiveConf(hconf, XmlSerializer.getUseCompression(hconf)),
        new SerializableObjectInspector(objectInspector))
  }
}

object GroupByPreShuffleOperator {
  private class GroupByPreShufflePartitionProcessor(
      private val conf: GroupByDesc,
      private val hconf: SerializableHiveConf,
      private val objectInspector: SerializableObjectInspector[ObjectInspector])
      extends PartitionProcessor with LogHelper {
    import shark.execution.GroupByOperator._
    
    override def processPartition(split: Int, iter: Iterator[_]) = {
      logInfo("Running Pre-Shuffle Group-By")
      
      val minReductionHashAggr = hconf.value.get(HiveConf.ConfVars.HIVEMAPAGGRHASHMINREDUCTION.varname).toFloat
      val numRowsCompareHashAggr = hconf.value.get(HiveConf.ConfVars.HIVEGROUPBYMAPINTERVAL.varname).toInt
      
      // The aggregation functions.
      val aggregationEvals = conf.getAggregators.map(_.getGenericUDAFEvaluator).toArray
      val aggregationIsDistinct = conf.getAggregators.map(_.getDistinct).toArray
      val rowInspector = objectInspector.value.asInstanceOf[StructObjectInspector]
      // Key fields to be grouped.
      val keyFields = conf.getKeys().map(k => ExprNodeEvaluatorFactory.get(k)).toArray
      val keyObjectInspectors: Array[ObjectInspector] = keyFields.map(k => k.initialize(rowInspector))
      val currentKeyObjectInspectors = SharkEnvSlave.objectInspectorLock.synchronized {
        keyObjectInspectors.map { k =>
          ObjectInspectorUtils.getStandardObjectInspector(k, ObjectInspectorCopyOption.WRITABLE)
        }
      }
  
      val aggregationParameterFields = conf.getAggregators.toArray.map { aggr =>
        aggr.asInstanceOf[AggregationDesc].getParameters.toArray.map { param =>
          ExprNodeEvaluatorFactory.get(param.asInstanceOf[ExprNodeDesc])
        }
      }
      val aggregationParameterObjectInspectors = aggregationParameterFields.map { aggr =>
        aggr.map { param => param.initialize(rowInspector) }
      }
      val aggregationParameterStandardObjectInspectors = aggregationParameterObjectInspectors.map { ois =>
        ois.map { oi =>
          ObjectInspectorUtils.getStandardObjectInspector(oi, ObjectInspectorCopyOption.WRITABLE)
        }
      }
  
      aggregationEvals.zipWithIndex.map { pair =>
        pair._1.init(conf.getAggregators.get(pair._2).getMode,
          aggregationParameterObjectInspectors(pair._2))
      }
  
      val keyFieldNames = conf.getOutputColumnNames.slice(0, keyFields.length)
      val totalFields = keyFields.length + aggregationEvals.length
      val keyois = new JArrayList[ObjectInspector](totalFields)
      keyObjectInspectors.foreach(keyois.add(_))
  
      // A struct object inspector composing of all the fields.
      val keyObjectInspector = SharkEnvSlave.objectInspectorLock.synchronized {
        ObjectInspectorFactory.getStandardStructObjectInspector(keyFieldNames, keyois)
      }
  
      val keyFactory = new KeyWrapperFactory(keyFields, keyObjectInspectors, currentKeyObjectInspectors)
      
      var numRowsInput = 0
      var numRowsHashTbl = 0
      var useHashAggr = true
  
      // Do aggregation on map side using hashAggregations hash table.
      val hashAggregations = new JHashMap[KeyWrapper, Array[AggregationBuffer]]()
  
      val newKeys: KeyWrapper = keyFactory.getKeyWrapper()
  
      while (iter.hasNext && useHashAggr) {
        val row = iter.next().asInstanceOf[AnyRef]
        numRowsInput += 1
  
        newKeys.getNewKey(row, rowInspector)
        newKeys.setHashKey()
  
        var aggs = hashAggregations.get(newKeys)
        var isNewKey = false
        if (aggs == null) {
          isNewKey = true
          val newKeyProber = newKeys.copyKey()
          aggs = newAggregations(aggregationEvals)
          hashAggregations.put(newKeyProber, aggs)
          numRowsHashTbl += 1
        }
        if (isNewKey) {
          aggregateNewKey(row, aggs, aggregationEvals, aggregationParameterFields)
        } else {
          aggregateExistingKey(row, aggs, aggregationIsDistinct, aggregationEvals, aggregationParameterFields)
        }
  
        // Disable partial hash-based aggregation if desired minimum reduction is
        // not observed after initial interval.
        if (numRowsInput == numRowsCompareHashAggr) {
          if (numRowsHashTbl > numRowsInput * minReductionHashAggr) {
            useHashAggr = false
            logInfo("Mapside hash aggregation disabled")
          } else {
            logInfo("Mapside hash aggregation enabled")
          }
          logInfo("#hash table="+numRowsHashTbl+" #rows="+
            numRowsInput+" reduction="+numRowsHashTbl.toFloat/numRowsInput+
            " minReduction="+minReductionHashAggr)
        }
      }
  
      // Generate an iterator for the aggregation output from hashAggregations.
      val outputCache = new Array[Object](keyFields.length + aggregationEvals.length)
      hashAggregations.toIterator.map { case(key, aggrs) =>
        val keyArr = key.getKeyArray()
        var i = 0
        while (i < keyArr.length) {
          outputCache(i) = keyArr(i)
          i += 1
        }
        i = 0
        while (i < aggrs.length) {
          outputCache(i + keyArr.length) = aggregationEvals(i).evaluate(aggrs(i))
          i += 1
        }
        outputCache
      } ++
      // Concatenate with iterator for remaining rows not in hashAggregations.
      iter.map { case row: AnyRef =>
        newKeys.getNewKey(row, rowInspector)
        val newAggrKey = newKeys.copyKey()
        val aggrs = newAggregations(aggregationEvals)
        aggregateNewKey(row, aggrs, aggregationEvals, aggregationParameterFields)
        val keyArr = newAggrKey.getKeyArray()
        var i = 0
        while (i < keyArr.length) {
          outputCache(i) = keyArr(i)
          i += 1
        }
        i = 0
        while (i < aggrs.length) {
          outputCache(i + keyArr.length) = aggregationEvals(i).evaluate(aggrs(i))
          i += 1
        }
        outputCache
      }
    }
  }
}
