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

import org.apache.hadoop.hive.ql.exec.{LimitOperator => HiveLimitOperator}

import shark.SharkEnv
import spark.RDD


class LimitOperator extends Operator[HiveLimitOperator] with UnaryOperator[HiveLimitOperator] {
  //HACK: Currently ExtractOperator relies on this, so it is exposed as public.
  // The code should be refactored so that this can be made private.
  def limit = hiveOp.getConf().getLimit()
  
  override def execute(): RDD[_] = {
    val limitNum = limit
    if (limitNum > 0) {
      // Take limit on each partition.
      val inputRdd = executeParents().head._2
      inputRdd.mapPartitions({ iter => iter.take(limitNum) }, preservesPartitioning = true)
    } else {
      new EmptyRDD(SharkEnv.sc)
    }
  }
}

