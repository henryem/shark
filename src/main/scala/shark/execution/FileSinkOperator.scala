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

import scala.reflect.BeanProperty
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{Operator => HiveOp, FileSinkOperator => HiveFileSinkOperator}
import org.apache.hadoop.hive.ql.exec.JobCloseFeedBack
import org.apache.hadoop.mapred.TaskID
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapred.HadoopWriter
import spark.RDD
import spark.TaskContext
import java.util.Date
import shark.execution.serialization.SerializableHiveConf
import shark.LogHelper
import shark.execution.serialization.SerializableHiveOperator
import shark.execution.serialization.SerializedHiveOperator
import shark.execution.serialization.XmlSerializer
import org.apache.hadoop.hive.ql.plan.FileSinkDesc


class FileSinkOperator extends Operator[HiveFileSinkOperator] with TerminalOperator {
  private def outputFileExtension: String = HiveConf.getVar(hconf, HiveConf.ConfVars.OUTPUT_FILE_EXTENSION)

  override def execute(): RDD[_] = {
    val rdd = if (parentOperators.size == 1) executeParents().head._2 else null
    val now = new Date()
    val thisString = this.toString()
    val operatorsString = this.objectInspectors.toString()
    val serializedHiveOp = SerializedHiveOperator.serialize(hiveOp, XmlSerializer.getUseCompression(hconf))
    val partitionProcessor = new FileSinkOperator.FileSinkPartitionProcessor(
        serializedHiveOp, new SerializableHiveConf(hconf, XmlSerializer.getUseCompression(hconf)), now, outputFileExtension, thisString, operatorsString)
    
    parentOperators.head match {
      case op: LimitOperator =>
        // If there is a limit operator, let's only run one partition at a time to avoid
        // launching too many tasks.
        val limit = op.limit
        var totalRows = 0
        var nextPartition = 0
        while (totalRows < limit) {
          // Run one partition and get back the number of rows processed there.
          totalRows += rdd.context.runJob(
            rdd,
            partitionProcessor.processPartition _,
            Seq(nextPartition),
            allowLocal = false).sum
          nextPartition += 1
        }

      case _ =>
        val rows = rdd.context.runJob(rdd, partitionProcessor.processPartition _)
        logInfo("Total number of rows written: " + rows.sum)
    }

    hiveOp.jobClose(hconf, true, new JobCloseFeedBack)
    rdd
  }
}


object FileSinkOperator {
  private class FileSinkPartitionProcessor(
      private val serializedHiveOp: SerializedHiveOperator[HiveFileSinkOperator],
      private val hiveConf: SerializableHiveConf,
      private val now: Date,
      private val outputFileExtension: String,
      private val opName: String,
      private val opObjectInspectorsName: String)
      extends Serializable with LogHelper {
    def processPartition(context: TaskContext, iter: Iterator[_]): Int = {
      logDebug("Started executing mapPartitions for operator: " + opName)
      logDebug("Input object inspectors: " + opObjectInspectorsName)

      setConfParams(context)
      val numRows = saveRows(iter)
      logDebug("Finished executing mapPartitions for operator: " + opName)
      numRows
    }
    
    // Set HiveConf parameters specific to the worker on which this processor
    // is running.  Note that a single HiveConf object is currently shared
    // across all code running on a single worker, so this method modifies
    // global state.  This is pretty bad, but it appears difficult to fix
    // without extensive changes to Hive.
    private def setConfParams(context: TaskContext) {
      val jobID = context.stageId
      val splitID = context.splitId
      val jID = HadoopWriter.createJobID(now, jobID)
      val taID = new TaskAttemptID(new TaskID(jID, true, splitID), 0)
      hiveConf.value.set("mapred.job.id", jID.toString)
      hiveConf.value.set("mapred.tip.id", taID.getTaskID.toString)
      hiveConf.value.set("mapred.task.id", taID.toString)
      hiveConf.value.setBoolean("mapred.task.is.map", true)
      hiveConf.value.setInt("mapred.task.partition", splitID)
  
      // Variables used by FileSinkOperator.
      if (outputFileExtension != null) {
        hiveConf.value.setVar(HiveConf.ConfVars.OUTPUT_FILE_EXTENSION, outputFileExtension)
      }
    }
    
    private def saveRows(iter: Iterator[_]): Int = {
      val hiveOp = serializedHiveOp.value
      var numRows = 0
  
      iter.foreach { row =>
        numRows += 1
        hiveOp.processOp(row, 0)
      }
  
      // Create missing parent directories so that the HiveFileSinkOperator can rename
      // temp file without complaining.
  
      // Two rounds of reflection are needed, since the FSPaths reference is private, and
      // the FSPaths' finalPaths reference isn't publicly accessible.
      val fspField = hiveOp.getClass.getDeclaredField("fsp")
      fspField.setAccessible(true)
      val fileSystemPaths = fspField.get(hiveOp).asInstanceOf[HiveFileSinkOperator#FSPaths]
  
      // File paths for dynamic partitioning are determined separately. See FileSinkOperator.java.
      if (fileSystemPaths != null) {
        val finalPathsField = fileSystemPaths.getClass.getDeclaredField("finalPaths")
        finalPathsField.setAccessible(true)
        val finalPaths = finalPathsField.get(fileSystemPaths).asInstanceOf[Array[Path]]
  
        // Get a reference to the FileSystem. No need for reflection here.
        val fileSystem = FileSystem.get(hiveConf.value)
  
        for (idx <- 0 until finalPaths.length) {
          var finalPath = finalPaths(idx)
          if (finalPath == null) {
            // If a query results in no output rows, then file paths for renaming will be
            // created in hiveOp.closeOp instead of processOp. But we need them before
            // that to check for missing parent directories.
            val createFilesMethod = hiveOp.getClass.getDeclaredMethod(
              "createBucketFiles", classOf[HiveFileSinkOperator#FSPaths])
            createFilesMethod.setAccessible(true)
            createFilesMethod.invoke(hiveOp, fileSystemPaths)
            finalPath = finalPaths(idx)
          }
          if (!fileSystem.exists(finalPath.getParent)) {
            fileSystem.mkdirs(finalPath.getParent)
          }
        }
      }
  
      hiveOp.asInstanceOf[HiveFileSinkOperator].closeOp(false)
      numRows
    }
  }
}
