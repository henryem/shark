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

import java.util.{ArrayList => JArrayList}
import org.apache.hadoop.hive.ql.exec.{GroupByOperator => HiveGroupByOperator}
import org.apache.hadoop.hive.ql.exec.{ReduceSinkOperator => HiveReduceSinkOperator}
import org.apache.hadoop.hive.ql.plan.GroupByDesc
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator
import org.apache.hadoop.hive.ql.exec.KeyWrapperFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import shark.execution.serialization.SerializableObjectInspector
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory
import shark.SharkEnvSlave
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.ql.plan.AggregationDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils


/**
 * Unlike Hive, group by in Shark is split into two different operators:
 * GroupByPostShuffleOperator and GroupByPreShuffleOperator. The pre-shuffle one
 * serves as a combiner on each map partition.
 *
 * These two classes are defined in org.apache.hadoop.hive.ql.exec package
 * (scala files) to get around the problem that some Hive classes are only
 * visible within that class.
 */
object GroupByOperator {
  
  def isPostShuffle(op: HiveGroupByOperator): Boolean = {
    op.getParentOperators().get(0).isInstanceOf[HiveReduceSinkOperator]
  }
  
  @inline final def aggregateNewKey(
      row: Object,
      aggregations: Array[AggregationBuffer],
      aggregationEvals: Array[GenericUDAFEvaluator],
      aggregationParameterFields: Array[Array[ExprNodeEvaluator]]) {
    var i = 0
    while (i < aggregations.length) {
      aggregationEvals(i).aggregate(
          aggregations(i), aggregationParameterFields(i).map(_.evaluate(row)))
          i += 1
    }
  }

  @inline final def aggregateExistingKey(
      row: AnyRef,
      aggregations: Array[AggregationBuffer],
      aggregationIsDistinct: Array[Boolean],
      aggregationEvals: Array[GenericUDAFEvaluator],
      aggregationParameterFields: Array[Array[ExprNodeEvaluator]]) {
    var i = 0
    while (i < aggregations.length) {
      if (!aggregationIsDistinct(i)) {
        aggregationEvals(i).aggregate(
            aggregations(i), aggregationParameterFields(i).map(_.evaluate(row)))
      }
      i += 1
    }
  }

  def newAggregations(aggregationEvals: Array[GenericUDAFEvaluator]): Array[AggregationBuffer] = {
    aggregationEvals.map(eval => eval.getNewAggregationBuffer)
  }
  
  @inline final def resetAggregations(aggs: Array[AggregationBuffer], aggregationEvals: Array[GenericUDAFEvaluator]) {
    var i = 0
    while (i < aggs.length) {
      aggregationEvals(i).reset(aggs(i))
      i += 1
    }
  }
  
  def createEmptyRow(
      aggregationEvals: Array[GenericUDAFEvaluator],
      aggregationParameterFields: Array[Array[ExprNodeEvaluator]]): Array[Object] = {
    val aggrs = newAggregations(aggregationEvals)
    val output = new Array[Object](aggrs.length)
    var i = 0
    while (i < aggrs.length) {
      var emptyObj: Array[Object] = null
      if (aggregationParameterFields(i).length > 0) {
        emptyObj = aggregationParameterFields.map { field => null }.toArray
      }
      aggregationEvals(i).aggregate(aggrs(i), emptyObj)
      output(i) = aggregationEvals(i).evaluate(aggrs(i))
      i += 1
    }
    output
  }
  
  /**
   * A container for common state used in group-by operations.  This contains
   * many values but is safe and efficient to serialize; only @conf and
   * @rowInspectorSer are actually serialized.
   */
  case class CommonGroupByState(conf: GroupByDesc, rowInspectorSer: SerializableObjectInspector[StructObjectInspector]) {
    // Currently this is implemented as a bunch of lazy variables.  This is
    // easy to implement, but perhaps not so easy to read.  Someone may want
    // to rewrite this in a procedural way.  I have attempted to order the
    // fields in the code according to a topological sort on their dependency
    // graph - earlier fields are required by later fields.
    import scala.collection.JavaConversions._
    
    @transient lazy val aggregationIsDistinct: Array[Boolean] = conf.getAggregators.map(_.getDistinct).toArray
    @transient lazy val rowInspector = rowInspectorSer.value
    @transient lazy val keyFields: Array[ExprNodeEvaluator] = conf.getKeys().map(k => ExprNodeEvaluatorFactory.get(k)).toArray
    @transient private lazy val keyObjectInspectors: Array[ObjectInspector] = keyFields.map(k => k.initialize(rowInspector))
    @transient private lazy val currentKeyObjectInspectors = SharkEnvSlave.objectInspectorLock.synchronized {
      keyObjectInspectors.map { k =>
        ObjectInspectorUtils.getStandardObjectInspector(k, ObjectInspectorCopyOption.WRITABLE)
      }
    }

    @transient lazy val aggregationParameterFields: Array[Array[ExprNodeEvaluator]] = conf.getAggregators.toArray.map { aggr =>
      aggr.asInstanceOf[AggregationDesc].getParameters.toArray.map { param =>
        ExprNodeEvaluatorFactory.get(param.asInstanceOf[ExprNodeDesc])
      }
    }
    
    @transient lazy val (aggregationParameterObjectInspectors, aggregationEvals): (Array[Array[ObjectInspector]], Array[GenericUDAFEvaluator]) = {
      val aggregationParameterObjectInspectorsTmp = aggregationParameterFields.map { aggr =>
        aggr.map { param => param.initialize(rowInspector) }
      }
      val aggregationEvalsTmp = conf.getAggregators.map(_.getGenericUDAFEvaluator).toArray
      aggregationEvalsTmp.zipWithIndex.foreach { pair =>
        pair._1.init(conf.getAggregators.get(pair._2).getMode,
          aggregationParameterObjectInspectorsTmp(pair._2))
      }
      (aggregationParameterObjectInspectorsTmp, aggregationEvalsTmp)
    }
    
    @transient lazy val aggregationParameterStandardObjectInspectors: Array[Array[ObjectInspector]] = aggregationParameterObjectInspectors.map { ois =>
      ois.map { oi =>
        ObjectInspectorUtils.getStandardObjectInspector(oi, ObjectInspectorCopyOption.WRITABLE)
      }
    }
    
    @transient lazy private val keyFieldNames = conf.getOutputColumnNames.slice(0, keyFields.length)
    @transient lazy private val totalFields = keyFields.length + aggregationEvals.length
    @transient lazy private val keyois = {
      val keyoisTmp = new JArrayList[ObjectInspector](totalFields)
      keyObjectInspectors.foreach(keyoisTmp.add(_))
      keyoisTmp
    }

    @transient lazy val keyObjectInspector: StructObjectInspector = SharkEnvSlave.objectInspectorLock.synchronized {
      ObjectInspectorFactory.getStandardStructObjectInspector(keyFieldNames, keyois)
    }

    @transient lazy val keyFactory: KeyWrapperFactory = new KeyWrapperFactory(keyFields, keyObjectInspectors, currentKeyObjectInspectors)
    
    // KEY has union field as the last field if there are distinct aggrs.
    @transient lazy val unionExprEvaluator: ExprNodeEvaluator = {
      val sfs = rowInspector.asInstanceOf[StructObjectInspector].getAllStructFieldRefs
      var unionExprEval: ExprNodeEvaluator = null
      if (sfs.size > 0) {
        val keyField = sfs.get(0)
        if (keyField.getFieldName.toUpperCase.equals(Utilities.ReduceField.KEY.name)) {
          val keyObjInspector = keyField.getFieldObjectInspector
          if (keyObjInspector.isInstanceOf[StandardStructObjectInspector]) {
            val keysfs = keyObjInspector.asInstanceOf[StructObjectInspector].getAllStructFieldRefs
            if (keysfs.size() > 0) {
              val sf = keysfs.get(keysfs.size() - 1)
              if (sf.getFieldObjectInspector().getCategory().equals(ObjectInspector.Category.UNION)) {
                unionExprEval = ExprNodeEvaluatorFactory.get(
                  new ExprNodeColumnDesc(
                    TypeInfoUtils.getTypeInfoFromObjectInspector(sf.getFieldObjectInspector),
                    keyField.getFieldName + "." + sf.getFieldName, null, false
                  )
                )
                unionExprEval.initialize(rowInspector)
              }
            }
          }
        }
      }
      unionExprEval
    }
  }
}

