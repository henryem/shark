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

import java.util.Date

import scala.reflect.BeanProperty

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator => HiveFileSinkOperator}


/**
 * A sink operator. It can accomplish one of the three things:
 * - write query output to disk
 * - cache query output
 * - return query as RDD directly (without materializing it)
 */
trait TerminalOperator extends UnaryOperator[HiveFileSinkOperator]

/**
 * Collect the output as a TableRDD.
 */
class TableRddSinkOperator extends SimpleUnaryOperator[HiveFileSinkOperator] with TerminalOperator {
  override def makePartitionProcessor() = PartitionProcessor.identity
}
