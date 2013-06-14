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

import java.util.{HashMap => JHashMap, List => JavaList, Map => JavaMap, Set => JavaSet}
import java.io.File
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.{Context, DriverContext}
import org.apache.hadoop.hive.ql.exec.{TableScanOperator => HiveTableScanOperator, Utilities}
import org.apache.hadoop.hive.ql.metadata.{Partition, Table}
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan.{PlanUtils, CreateTableDesc, PartitionDesc, ExprNodeDesc}
import org.apache.hadoop.hive.ql.plan.api.StageType
import org.apache.hadoop.hive.ql.session.SessionState
import scala.collection.JavaConversions._
import shark.api.TableRDD
import shark.{LogHelper, SharkEnv}
import spark.RDD
import java.io.InvalidObjectException
import shark.execution.serialization.SerializableWritable


class SparkWork(
    val topToTable: JavaMap[HiveTableScanOperator, Table],
    val opToPartPruner: JavaMap[HiveTableScanOperator, ExprNodeDesc],
    val confWrapper: SerializableWritable[HiveConf],
    val prunedPartitionListWrapper: Map[String, PrunedPartitionListSerializationWrapper],
    val terminalOperator: TerminalOperator,
    val resultSchema: JavaList[FieldSchema])
    extends java.io.Serializable {
  def prunedPartitionList(): JavaMap[String, PrunedPartitionList] = prunedPartitionListWrapper.mapValues(_.prunedPartitionList)
  def conf(): HiveConf = confWrapper.t
}

object SparkWork {
  /** Convenience constructor from a ParseContext and other necessary data. */
  def fromParseContext(pctx: ParseContext, terminalOperator: TerminalOperator, resultSchema: JavaList[FieldSchema]): SparkWork = {
    new SparkWork(
        pctx.getTopToTable(),
        pctx.getOpToPartPruner(),
        new SerializableWritable(pctx.getConf()),
        pctx.getPrunedPartitions().mapValues(ppl => new PrunedPartitionListSerializationWrapper(ppl)).toMap,
        terminalOperator,
        resultSchema)
  }
}

/** PrunedPartitionList is not serializable; this wrapper makes it so. */
class PrunedPartitionListSerializationWrapper(
    @transient val prunedPartitionList: PrunedPartitionList)
    extends java.io.Serializable {
  import PrunedPartitionListSerializationWrapper._
  
  private def writeReplace(): Object
    = new SerializationProxy(
        prunedPartitionList.getConfirmedPartns(),
        prunedPartitionList.getUnknownPartns(),
        prunedPartitionList.getDeniedPartns())
  
  private def readResolve(): Object
    = throw new InvalidObjectException("Attempted to deserialize an object rather than its serialization proxy.")
}

object PrunedPartitionListSerializationWrapper {
  private class SerializationProxy(
      private val confirmedPartns: JavaSet[Partition],
      private val unknownPartns: JavaSet[Partition],
      private val deniedPartns: JavaSet[Partition]
      ) extends java.io.Serializable {
    private def readResolve(): Object
      = new PrunedPartitionListSerializationWrapper(new PrunedPartitionList(
          confirmedPartns, unknownPartns, deniedPartns))
  }
}

/**
 * SparkTask executes a query plan composed of RDD operators.
 */
class SparkTask extends org.apache.hadoop.hive.ql.exec.Task[SparkWork]
with java.io.Serializable with LogHelper {

  private var _tableRdd: TableRDD = null
  def tableRdd = _tableRdd

  override def execute(driverContext: DriverContext): Int = {
    logInfo("Executing " + this.getClass.getName)

    val ctx = driverContext.getCtx()

    // Adding files to the SparkContext
    // Added required files
    val files = Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE)
    files.split(",").filterNot(x => x.isEmpty || SharkEnv.addedFiles.contains(x)).foreach { x =>
      logInfo("Adding file "  + x )
      SharkEnv.addedFiles.add(x)
      SharkEnv.sc.addFile(x)
    }

    // Added required jars
    val jars = Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR)
    jars.split(",").filterNot(x => x.isEmpty || SharkEnv.addedJars.contains(x)).foreach { x =>
      logInfo("Adding jar "  + x )
      SharkEnv.addedJars.add(x)
      SharkEnv.sc.addJar(x)
    }

    Operator.hconf = conf

    // Replace Hive physical plan with Shark plan.
    val terminalOp = work.terminalOperator
    val tableScanOps = terminalOp.returnTopOperators().asInstanceOf[Seq[TableScanOperator]]

    //ExplainTaskHelper.outputPlan(terminalOp, Console.out, true, 2)
    //ExplainTaskHelper.outputPlan(hiveTopOps.head, Console.out, true, 2)

    SparkTask.initializeTableScanTableDesc(tableScanOps, work)

    // Initialize the Hive query plan. This gives us all the object inspectors.
    SparkTask.initializeAllHiveOperators(terminalOp)

    terminalOp.initializeMasterOnAll()

    val sinkRdd = terminalOp.execute().asInstanceOf[RDD[Any]]

    _tableRdd = new TableRDD(sinkRdd, work.resultSchema, terminalOp.objectInspector)
    0
  }

  override def getType = StageType.MAPRED

  override def getName = "MAPRED-SPARK"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit

}

object SparkTask {
  
  def initializeTableScanTableDesc(topOps: Seq[TableScanOperator], work: SparkWork) {
    // topToTable maps Hive's TableScanOperator to the Table object.
    val topToTable: JavaMap[HiveTableScanOperator, Table] = work.topToTable

    // Add table metadata to TableScanOperators
    topOps.foreach { op =>
      op.table = topToTable.get(op.hiveOp)
      op.tableDesc = Utilities.getTableDesc(op.table)
      PlanUtils.configureInputJobPropertiesForStorageHandler(op.tableDesc)
      if (op.table.isPartitioned) {
        val ppl = PartitionPruner.prune(
          op.table,
          work.opToPartPruner.get(op.hiveOp),
          work.conf, "",
          work.prunedPartitionList)
        op.parts = ppl.getConfirmedPartns.toArray ++ ppl.getUnknownPartns.toArray
        val allParts = op.parts ++ ppl.getDeniedPartns.toArray
        if (allParts.size == 0) {
          op.firstConfPartDesc = new PartitionDesc(op.tableDesc, null)
        } else {
          op.firstConfPartDesc = Utilities.getPartitionDesc(allParts(0).asInstanceOf[Partition])
        }
      }
    }
  }
  
  def initializeAllHiveOperators(terminalOp: TerminalOperator) = {
    // Need to guarantee all parents are initialized before the child.
    val topOpList = new scala.collection.mutable.MutableList[HiveTopOperator]
    val queue = new scala.collection.mutable.Queue[Operator[_]]
    queue.enqueue(terminalOp)

    while (!queue.isEmpty) {
      val current = queue.dequeue()
      current match {
        case op: HiveTopOperator => topOpList += op
        case _ => Unit
      }
      queue ++= current.parentOperators
    }

    // Run the initialization. This guarantees that upstream operators are
    // initialized before downstream ones.
    topOpList.reverse.foreach { topOp =>
      topOp.initializeHiveTopOperator()
    }
  }
}

