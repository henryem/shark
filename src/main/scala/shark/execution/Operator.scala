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
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{Operator => HiveOp}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import shark.LogHelper
import spark.RDD
import shark.execution.serialization.SerializableWritable
import shark.execution.serialization.SerializableHiveOperator
import shark.execution.serialization.SerializedHiveOperator
import shark.execution.serialization.XmlSerializer
import shark.execution.serialization.SerializableObjectInspector
import shark.execution.serialization.SerializableObjectInspectors
import shark.execution.serialization.HiveOperatorWrapper
import shark.execution.serialization.HiveOperatorSerialization
import shark.execution.serialization.SerializableHiveConf
import com.google.common.util.concurrent.SettableFuture
import com.google.common.util.concurrent.Futures
import java.util.concurrent.Future


trait Operator[T <: HiveOp[_]] extends LogHelper with Serializable {
//  private var _hiveOp: HiveOperatorWrapper[T] = _
  private var _hiveOp: T = _
  private val _childOperators = new ArrayBuffer[Operator[_]]()
  private val _parentOperators = new ArrayBuffer[Operator[_]]()
  private var _objectInspectors: SerializableObjectInspectors[ObjectInspector] = new SerializableObjectInspectors(Array.empty)
  
  /**
   * Execute the operator. This should recursively execute parent operators.
   */
  def execute(): RDD[_]

  /**
   * Recursively calls initializeOnMaster() for the entire query plan. Parent
   * operators are called before children.
   */
  def initializeMasterOnAll() {
    _parentOperators.foreach(_.initializeMasterOnAll())
    _objectInspectors = new SerializableObjectInspectors(_objectInspectors.value ++ hiveOp.getInputObjInspectors())
  }

  /**
   * Return the join tag. This is usually just 0. ReduceSink might set it to
   * something else.
   */
  def getTag: Int = 0

  def hconf = Operator.hconf
  
  //FIXME: Currently hconf is not available when hiveOp is being set, so
  // this is a quick hack around that.  This means Operator is not serializable.
  def hiveOp: T = _hiveOp
  def hiveOp_=(newHiveOp: T) { _hiveOp = newHiveOp }
//  def hiveOp: T = _hiveOp.value
//  def hiveOp_=(newHiveOp: T) {
////    _hiveOp = HiveOperatorSerialization.serialize(newHiveOp, hconf, XmlSerializer.getUseCompression(hconf))
//  }
  
  def objectInspectors = _objectInspectors.value

  def childOperators = _childOperators
  def parentOperators = _parentOperators

  /**
   * Return the parent operators as a Java List. This is for interoperability
   * with Java. We use this in explain's Java code.
   */
  def parentOperatorsAsJavaList: JavaList[Operator[_]] = _parentOperators

  def addParent(parent: Operator[_]) {
    _parentOperators += parent
    parent.childOperators += this
  }

  def addChild(child: Operator[_]) {
    child.addParent(this)
  }

  def returnTerminalOperators(): Seq[Operator[_]] = {
    if (_childOperators == null || _childOperators.size == 0) {
      Seq(this)
    } else {
      _childOperators.flatMap(_.returnTerminalOperators())
    }
  }

  def returnTopOperators(): Seq[Operator[_]] = {
    if (_parentOperators == null || _parentOperators.size == 0) {
      Seq(this)
    } else {
      _parentOperators.flatMap(_.returnTopOperators())
    }
  }

  protected def executeParents(): Seq[(Int, RDD[_])] = {
    parentOperators.map(p => (p.getTag, p.execute()))
  }
}


/**
 * A base operator class that has many parents and one child. This can be used
 * to implement join, union, etc. Operator implementations should override the
 * following methods:
 *
 * combineMultipleRdds: Combines multiple RDDs into a single RDD. E.g. in the
 * case of join, this function does the join operation.
 *
 * makePartitionProcessor: Called on the master. The result will be serialized
 * and sent to each slave, where it will be used to transform each partition
 * of the output of combineMultipleRdds.
 *
 * postprocessRdd: Called on the master to transform the output of
 * processPartition before sending it downstream.
 *
 */
abstract class SimpleNaryOperator[T <: HiveOperator] extends Operator[T] with NaryOperator[T] {

  /** 
   * Make a PartitionProcessor for this operator.  This processor's
   * processPartition method will be mapped over the partitions of the input
   * RDD for this operator to produce the output RDD.
   */
  def makePartitionProcessor(): PartitionProcessor

  /** Called on master. */
  def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_]

  /** Called on master. */
  def postprocessRdd(rdd: RDD[_]): RDD[_] = rdd

  override final def execute(): RDD[_] = {
    val inputRdds = executeParents()
    val singleRdd = combineMultipleRdds(inputRdds)
    val rddProcessed = PartitionProcessor.executeProcessPartition(makePartitionProcessor(), singleRdd, this.toString(), objectInspectors.toString())
    postprocessRdd(rddProcessed)
  }

}

/** Marker trait for operators with many parents and one or fewer children. */
trait NaryOperator[T <: HiveOperator] extends Operator[T]


/**
 * A base operator class that has at most one parent.
 * Operators implementations should override the following methods:
 *
 * preprocessRdd: Called on the master. Can be used to transform the RDD before
 * passing it to processPartition. For example, the operator can use this
 * function to sort the input.
 *
 * makePartitionProcessor: Called on the master. The result will be serialized
 * and sent to each slave, where it will be used to transform each partition
 * of the output of preprocessRdd.
 *
 * postprocessRdd: Called on the master to transform the output of
 * processPartition before sending it downstream.
 *
 */
abstract class SimpleUnaryOperator[T <: HiveOperator] extends Operator[T] with UnaryOperator[T] {

  /** 
   * Make a PartitionProcessor for this operator.  This processor's
   * processPartition method will be mapped over the partitions of the input
   * RDD for this operator to produce the output RDD.
   */
  def makePartitionProcessor(): PartitionProcessor

  //TODO: Not clear if this is needed, and it's a little messy.
  /** Called on master. */
  def preprocessRdd(rdd: RDD[_]): RDD[_] = rdd

  //TODO: Not clear if this is needed, and it's a little messy.
  /** Called on master. */
  def postprocessRdd(rdd: RDD[_]): RDD[_] = rdd

  def objectInspector = objectInspectors.head

  def parentOperator = parentOperators.head

  override def execute(): RDD[_] = {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null
    val rddPreprocessed = preprocessRdd(inputRdd)
    val rddProcessed = PartitionProcessor.executeProcessPartition(makePartitionProcessor(), rddPreprocessed, this.toString(), objectInspectors.toString())
    postprocessRdd(rddProcessed)
  }
}

/** Marker trait for operators with one parent and one or fewer children. */
trait UnaryOperator[T <: HiveOperator] extends Operator[T]

/** Marker trait for operators with no parents. */
trait TopOperator[T <: HiveOperator] extends Operator[T]


object Operator extends LogHelper {

  /** A reference to the global HiveConf used by all Operators. */
  //FIXME: Replace with a default-constructed HiveConf.
  @transient private var _hconfFuture: SettableFuture[SerializableHiveConf] = SettableFuture.create()
  def hconf = _hconfFuture.get.value
  def hconf_=(newHconf: HiveConf) {
    //FIXME: Use compression?
    //FIXME: 
    _hconfFuture.set(new SerializableHiveConf(newHconf, true))
  }
  def getHconfFuture: Future[SerializableHiveConf] = _hconfFuture
}

