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

import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer
import scala.reflect.BeanProperty
import org.apache.hadoop.io.Writable
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator => HiveFileSinkOperator}
import org.apache.hadoop.hive.ql.plan.FileSinkDesc
import shark.{SharkConfVars, SharkEnv, SharkEnvSlave, Utils}
import shark.execution.serialization.{JavaSerializer, SerializableObjectInspector}
import shark.memstore2._
import shark.tachyon.TachyonTableWriter
import spark.{RDD, TaskContext}
import spark.SparkContext._
import spark.storage.StorageLevel
import spark.Accumulable
import shark.execution.serialization.SerializableHiveConf
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import shark.execution.serialization.XmlSerializer
import org.apache.commons.lang.SerializationUtils


/**
 * Cache the RDD and force evaluate it (so the cache is filled).
 */
class MemoryStoreSinkOperator extends Operator[HiveFileSinkOperator] with TerminalOperator {
  //NOTE: These are really parameters of this operator.  Unfortunately we need
  // to have no-arg constructors because operators are instantiated via
  // reflection, so the parameters are set after the operator is constructed.
  // This seems like a hack.
  //TODO: Make sure StorageLevel is serializable.
  private var storageLevel: StorageLevel = _
  private var tableName: String = _
  private var useTachyon: Boolean = _
  private var useUnionRDD: Boolean = _
  private var numColumns: Int = _

  //HACK
  def setParameters(
      newStorageLevel: StorageLevel,
      newTableName: String,
      newUseTachyon: Boolean,
      newUseUnionRDD: Boolean,
      newNumColumns: Int) {
    storageLevel = newStorageLevel
    tableName = newTableName
    useTachyon = newUseTachyon
    useUnionRDD = newUseUnionRDD
    numColumns = newNumColumns
  }
  
  override def execute(): RDD[_] = {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null

    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())
    
    val tachyonWriter: Option[TachyonTableWriter] =
      if (useTachyon) {
        // Use an additional row to store metadata (e.g. number of rows in each partition).
        Some(SharkEnv.tachyonUtil.createTableWriter(tableName, numColumns + 1))
      } else {
        None
      }

    val initialColumnSize = SharkConfVars.getIntVar(hconf, SharkConfVars.COLUMN_INITIALSIZE)
    
    val partitionConsolidator = new MemoryStoreSinkOperator.PartitionConsolidator(
        new SerializableHiveConf(hconf, XmlSerializer.getUseCompression(hconf)),
        hiveOp.getConf(),
        statsAcc,
        new SerializableObjectInspector(objectInspectors.head),
        initialColumnSize)
    inputRdd.foreach({_ => Unit}) //TMP FIXME
    // Put all rows of the table into a set of TablePartitions. Each partition contains
    // only one TablePartition object.
    //FIXME: Make PartitionProcessor parametric so that we can have static typing.
    val rdd: RDD[TablePartition] = inputRdd.mapPartitionsWithIndex({ case (idx, iter) =>
      partitionConsolidator.processPartition(idx, iter).asInstanceOf[Iterator[TablePartition]]
    })

    val writtenRdd = if (tachyonWriter.isDefined) {
      // Put the table in Tachyon.
      logInfo("Putting RDD for %s in Tachyon".format(tableName))

      SharkEnv.memoryMetadataManager.put(tableName, rdd)

      tachyonWriter.get.createTable(ByteBuffer.allocate(0))
      val tachyonWriteProcessor = new MemoryStoreSinkOperator.TachyonWritingProcessor(new SerializableHiveConf(hconf, XmlSerializer.getUseCompression(hconf)), initialColumnSize, tachyonWriter.get)
      val tachyonWrittenRdd = rdd.mapPartitionsWithIndex(tachyonWriteProcessor.processPartition _)
      // Force evaluate so the data gets put into Tachyon.
      tachyonWrittenRdd.foreach(_ => Unit)
      tachyonWrittenRdd
    } else {
      // Put the table in Spark block manager.
      logInfo("Putting %sRDD for %s in Spark block manager, %s %s %s %s".format(
        if (useUnionRDD) "Union" else "",
        tableName,
        if (storageLevel.deserialized) "deserialized" else "serialized",
        if (storageLevel.useMemory) "in memory" else "",
        if (storageLevel.useMemory && storageLevel.useDisk) "and" else "",
        if (storageLevel.useDisk) "on disk" else ""))

      // Force evaluate so the data gets put into Spark block manager.
      rdd.persist(storageLevel)
      rdd.foreach(_ => Unit)

      val postUnionRdd = if (useUnionRDD) {
        rdd.union(
          SharkEnv.memoryMetadataManager.get(tableName).get.asInstanceOf[RDD[TablePartition]])
      } else {
        rdd
      }
      SharkEnv.memoryMetadataManager.put(tableName, postUnionRdd)
      postUnionRdd
    }

    // Report remaining memory.
    /* Commented out for now waiting for the reporting code to make into Spark.
    val remainingMems: Map[String, (Long, Long)] = SharkEnv.sc.getSlavesMemoryStatus
    remainingMems.foreach { case(slave, mem) =>
      println("%s: %s / %s".format(
        slave,
        Utils.memoryBytesToString(mem._2),
        Utils.memoryBytesToString(mem._1)))
    }
    println("Summary: %s / %s".format(
      Utils.memoryBytesToString(remainingMems.map(_._2._2).sum),
      Utils.memoryBytesToString(remainingMems.map(_._2._1).sum)))
    */

    val columnStats =
      if (useUnionRDD) {
        // Combine stats for the two tables being combined.
        val numPartitions = statsAcc.value.toMap.size
        val currentStats = statsAcc.value
        val otherIndexToStats = SharkEnv.memoryMetadataManager.getStats(tableName).get
        for ((otherIndex, tableStats) <- otherIndexToStats) {
          currentStats.append((otherIndex + numPartitions, tableStats))
        }
        currentStats.toMap
      } else {
        statsAcc.value.toMap
      }

    // Get the column statistics back to the cache manager.
    SharkEnv.memoryMetadataManager.putStats(tableName, columnStats)

    if (tachyonWriter.isDefined) {
      tachyonWriter.get.updateMetadata(ByteBuffer.wrap(JavaSerializer.serialize(columnStats)))
    }

    if (SharkConfVars.getBoolVar(hconf, SharkConfVars.MAP_PRUNING_PRINT_DEBUG)) {
      columnStats.foreach { case(index, tableStats) =>
        println("Partition " + index + " " + tableStats.toString)
      }
    }

    // Return the cached RDD.
    writtenRdd
  }
}

object MemoryStoreSinkOperator {
  private class PartitionConsolidator(
      private val hconf: SerializableHiveConf,
      private val conf: FileSinkDesc,
      private val statsAcc: Accumulable[ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)],
      private val objectInspector: SerializableObjectInspector[ObjectInspector],
      private val initialColumnSize: Int
      ) extends PartitionProcessor {
    override def processPartition(partitionIndex: Int, iter: Iterator[_]): Iterator[_] = {
      hconf.value.setInt(SharkConfVars.COLUMN_INITIALSIZE.varname, initialColumnSize)
      
      val serde = new ColumnarSerDe
      serde.initialize(hconf.value, conf.getTableInfo.getProperties())

      // Serialize each row into the builder object.
      // ColumnarSerDe will return a TablePartitionBuilder.
      var builder: Writable = null
      iter.foreach { row =>
        builder = serde.serialize(row.asInstanceOf[AnyRef], objectInspector.value)
      }

      if (builder != null) {
        statsAcc += Tuple2(partitionIndex, builder.asInstanceOf[TablePartitionBuilder].stats)
        Iterator(builder.asInstanceOf[TablePartitionBuilder].build)
      } else {
        // Empty partition.
        statsAcc += Tuple2(partitionIndex, new TablePartitionStats(Array(), 0))
        Iterator(new TablePartition(0, Array()))
      }
    }
  }
  
  private class TachyonWritingProcessor(
      private val hconf: SerializableHiveConf,
      private val initialColumnSize: Int,
      private val tachyonWriter: TachyonTableWriter
      ) extends PartitionProcessor {
    override def processPartition(partitionIndex: Int, iter: Iterator[_]): Iterator[_] = {
      hconf.value.setInt(SharkConfVars.COLUMN_INITIALSIZE.varname, initialColumnSize)
      
      //HACK: Should type-parameterize PartitionProcessor instead of casting
      // here.
      val partition = iter.next().asInstanceOf[TablePartition]
      partition.toTachyon.zipWithIndex.foreach { case(buf, column) =>
        tachyonWriter.writeColumnPartition(column, partitionIndex, buf)
      }
      Iterator(partition)
    }
  }
}