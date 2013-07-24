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

import java.util.{ArrayList, Arrays}
import scala.reflect.BeanProperty
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS
import org.apache.hadoop.hive.ql.exec.{TableScanOperator => HiveTableScanOperator}
import org.apache.hadoop.hive.ql.exec.{Utilities}
import org.apache.hadoop.hive.ql.metadata.{Partition, Table}
import org.apache.hadoop.hive.ql.plan.{PlanUtils, PartitionDesc, TableDesc}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory,
  StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Writable
import shark.{SharkConfVars, SharkEnv, Utils}
import shark.execution.serialization.{XmlSerializer, JavaSerializer}
import shark.memstore2.{CacheType, TablePartition, TablePartitionStats}
import shark.tachyon.TachyonException
import spark.RDD
import spark.rdd.{PartitionPruningRDD, UnionRDD}
import org.apache.hadoop.hive.ql.io.HiveInputFormat
import shark.execution.serialization.{SerializableObjectInspector, SerializableHiveConf}
import org.apache.hadoop.hive.ql.exec.MapSplitPruning


class TableScanOperator extends UnaryOperator[HiveTableScanOperator]
    with TopOperator[HiveTableScanOperator]
    with HiveTopOperator[HiveTableScanOperator] {
  private var table: Table = _
  private var tableDesc: TableDesc = _
  private var parts: Option[Array[Partition]] = _
  private var firstConfPartDesc: Option[PartitionDesc]  = _
  
  private val inputObjectInspectors = new scala.collection.mutable.HashMap[Int, SerializableObjectInspector[ObjectInspector]]

  override def setKeyValueTableDescs(tag: Int, descs: (TableDesc, TableDesc)) {}
  
  override def setInputObjectInspector(tag: Int, objectInspector: ObjectInspector) {
    inputObjectInspectors.put(tag, new SerializableObjectInspector(objectInspector))
  }
  
  /**
   * Set some properties of the table to be scanned.  This must be called
   * before initializeHiveTopOperator() or execute().  Currently it is called
   * in SparkTask.execute().
   * 
   * HACK: We should have a cleaner way of doing this.
   */
  def setTableProperties(
      table: Table,
      tableDesc: TableDesc,
      parts: Option[Array[Partition]],
      firstConfPartDesc: Option[PartitionDesc]) {
    this.table = table
    this.tableDesc = tableDesc
    this.parts = parts
    this.firstConfPartDesc = firstConfPartDesc
  }
  
  /**
   * Initialize the hive TableScanOperator. This initialization propagates
   * downstream. When all Hive TableScanOperators are initialized, the entire
   * Hive query plan operators are initialized.
   */
  override def initializeHiveTopOperator() {
    require(table != null && tableDesc != null && parts != null && firstConfPartDesc != null,
        "TableScanOperator cannot be used or initialized until setTableProperties() has been called.")
    val rowObjectInspector = {
      if (!parts.isDefined) {
        val serializer = tableDesc.getDeserializerClass().newInstance()
        serializer.initialize(hconf, tableDesc.getProperties)
        serializer.getObjectInspector()
      } else {
        require(firstConfPartDesc.isDefined)
        val partProps = firstConfPartDesc.get.getProperties()
        val tableDeser = firstConfPartDesc.get.getDeserializerClass().newInstance()
        tableDeser.initialize(hconf, partProps)
        val partCols = partProps.getProperty(META_TABLE_PARTITION_COLUMNS)
        val partNames = new ArrayList[String]
        val partObjectInspectors = new ArrayList[ObjectInspector]
        partCols.trim().split("/").foreach{ key =>
          partNames.add(key)
          partObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
        }

        // No need to lock this one (see SharkEnv.objectInspectorLock) because
        // this is called on the master only.
        val partObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
            partNames, partObjectInspectors)
        val oiList = Arrays.asList(
            tableDeser.getObjectInspector().asInstanceOf[StructObjectInspector],
            partObjectInspector.asInstanceOf[StructObjectInspector])
        // new oi is union of table + partition object inspectors
        ObjectInspectorFactory.getUnionStructObjectInspector(oiList)
      }
    }

    setInputObjectInspector(0, rowObjectInspector)
    HiveTopOperator.initializeHiveTopOperator(this, inputObjectInspectors.mapValues(_.value).toMap)
  }

  override def execute(): RDD[_] = {
    assert(parentOperators.size == 0)
    val tableKey: String = tableDesc.getTableName.split('.')(1)

    // There are three places we can load the table from.
    // 1. Tachyon table
    // 2. Spark heap (block manager)
    // 3. Hive table on HDFS (or other Hadoop storage)

    val cacheMode = CacheType.fromString(
      tableDesc.getProperties().get("shark.cache").asInstanceOf[String])
    cacheMode match {
      case CacheType.heap => loadFromSpark(tableKey)
      case CacheType.tachyon => loadFromTachyon(tableKey)
      case _ => loadFromHive()
    }
  }
  
  private def loadFromTachyon(tableKey: String): RDD[_] = {
    if (!SharkEnv.tachyonUtil.tableExists(tableKey)) {
      throw new TachyonException("Table " + tableKey + " does not exist in Tachyon")
    }
    logInfo("Loading table " + tableKey + " from Tachyon")

    var indexToStats: collection.Map[Int, TablePartitionStats] =
    SharkEnv.memoryMetadataManager.getStats(tableKey).getOrElse(null)

    if (indexToStats == null) {
      val statsByteBuffer = SharkEnv.tachyonUtil.getTableMetadata(tableKey)
      indexToStats = JavaSerializer.deserialize[collection.Map[Int, TablePartitionStats]](
          statsByteBuffer.array())
      SharkEnv.memoryMetadataManager.putStats(tableKey, indexToStats)
    }
    SharkEnv.tachyonUtil.createRDD(tableKey)
  }

  private def loadFromSpark(tableKey: String): RDD[_] = {
    val rdd = SharkEnv.memoryMetadataManager.get(tableKey).get
    logInfo("Loading table " + tableKey + " from Spark block manager")
    
    // Stats used for map pruning.
    val indexToStats: collection.Map[Int, TablePartitionStats] =
      SharkEnv.memoryMetadataManager.getStats(tableKey).get

    // Run map pruning if the flag is set, there exists a filter predicate on
    // the input table and we have statistics on the table.
    val prunedRdd: RDD[_] =
      if (SharkConfVars.getBoolVar(hconf, SharkConfVars.MAP_PRUNING) &&
          childOperators(0).isInstanceOf[FilterOperator] &&
          indexToStats.size == rdd.partitions.size) {

        val startTime = System.currentTimeMillis
        val printPruneDebug = SharkConfVars.getBoolVar(
          hconf, SharkConfVars.MAP_PRUNING_PRINT_DEBUG)

        val filterOp = childOperators(0).asInstanceOf[FilterOperator]

        def prunePartitionFunc(index: Int): Boolean = {
          if (printPruneDebug) {
            logInfo("\nPartition " + index + "\n" + indexToStats(index))
          }
          // Only test for pruning if we have stats on the column.
          val partitionStats = indexToStats(index)
          if (partitionStats != null && partitionStats.stats != null)
            MapSplitPruning.test(partitionStats, filterOp.getExprNodeEvaluator)
          else true
        }

        // Do the pruning.
        val prunedRdd = PartitionPruningRDD.create(rdd, prunePartitionFunc)
        val timeTaken = System.currentTimeMillis - startTime
        logInfo("Map pruning %d partitions into %s partitions took %d ms".format(
            rdd.partitions.size, prunedRdd.partitions.size, timeTaken))
        prunedRdd
      } else {
        rdd
      }

    prunedRdd.mapPartitions { iter =>
      if (iter.hasNext) {
        val tablePartition = iter.next.asInstanceOf[TablePartition]
        tablePartition.iterator
      } else {
        Iterator()
      }
    }
  }
  
  private def loadFromHive(): RDD[_] = {
    val deserializer = new TableScanOperator.HiveDeserializingProcessor(tableDesc, new SerializableHiveConf(hconf, XmlSerializer.getUseCompression(hconf)))
    getRdd().mapPartitionsWithIndex(deserializer.processPartition _)
  }

  /**
   * Create a RDD representing the table (with or without partitions).
   * The result may still require deserialization of its rows.
   */
  private def getRdd(): RDD[_] = {
    if (table.isPartitioned) {
      logInfo("Making %d Hive partitions".format(parts.get.size))
      makePartitionRDD()
    } else {
      val tablePath = table.getPath.toString
      val ifc = table.getInputFormatClass
          .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
      logInfo("Table input: %s".format(tablePath))
      createHadoopRdd(tablePath, ifc)
    }
  }

  private def makePartitionRDD[T](): RDD[_] = {
    val partitions = parts.get
    val rdds = new Array[RDD[Any]](partitions.size)

    var i = 0
    partitions.foreach { part =>
      val partition = part.asInstanceOf[Partition]
      val partDesc = Utilities.getPartitionDesc(partition)
      val tablePath = partition.getPartitionPath.toString

      val ifc = partition.getInputFormatClass
        .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
      val hadoopRdd = createHadoopRdd(tablePath, ifc)

      //TODO: Use a PartitionProcessor and a SerializableHiveConf just to make
      // this more consistent with similar code elsewhere, and to make it clear
      // what is being serialized.
      val serializedHconf = XmlSerializer.serialize(hconf, XmlSerializer.getUseCompression(hconf))
      val partRDD = hadoopRdd.mapPartitions { iter =>
        // Map each tuple to a row object
        val hconf = XmlSerializer.deserialize(serializedHconf).asInstanceOf[HiveConf]
        val deserializer = partDesc.getDeserializerClass().newInstance()
        deserializer.initialize(hconf, partDesc.getProperties())

        // Get partition field info
        val partSpec = partDesc.getPartSpec()
        val partProps = partDesc.getProperties()

        val partCols = partProps.getProperty(META_TABLE_PARTITION_COLUMNS)
        val partKeys = partCols.trim().split("/")
        val partValues = new ArrayList[String]
        partKeys.foreach { key =>
          if (partSpec == null) {
            partValues.add(new String)
          } else {
            partValues.add(new String(partSpec.get(key)))
          }
        }

        val rowWithPartArr = new Array[Object](2)
        iter.map { value =>
          val deserializedRow = deserializer.deserialize(value) // LazyStruct
          rowWithPartArr.update(0, deserializedRow)
          rowWithPartArr.update(1, partValues)
          rowWithPartArr.asInstanceOf[Object]
        }
      }
      rdds(i) = partRDD.asInstanceOf[RDD[Any]]
      i += 1
    }
    // Even if we don't use any partitions, we still need an empty RDD
    if (rdds.size == 0) {
      SharkEnv.sc.makeRDD(Seq[Object]())
    } else {
      new UnionRDD(rdds(0).context, rdds)
    }
  }

  private def createHadoopRdd(
      path: String,
      ifc: Class[InputFormat[Writable, Writable]]):
      RDD[Writable] = {
    val conf = new JobConf(hconf)
    if (tableDesc != null) {
      Utilities.copyTableJobPropertiesToConf(tableDesc, conf)
    }
    new HiveInputFormat() {
      def doPushFilters() {
        pushFilters(conf, hiveOp)
      }
    }.doPushFilters()
    FileInputFormat.setInputPaths(conf, path)
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    conf.set("io.file.buffer.size", bufferSize)

    // Set s3/s3n credentials. Setting them in conf ensures the settings propagate
    // from Spark's master all the way to Spark's slaves.
    var s3varsSet = false
    val s3vars = Seq("fs.s3n.awsAccessKeyId", "fs.s3n.awsSecretAccessKey",
      "fs.s3.awsAccessKeyId", "fs.s3.awsSecretAccessKey").foreach { variableName =>
      if (hconf.get(variableName) != null) {
        s3varsSet = true
        conf.set(variableName, hconf.get(variableName))
      }
    }

    // If none of the s3 credentials are set in Hive conf, try use the environmental
    // variables for credentials.
    if (!s3varsSet) {
      Utils.setAwsCredentials(conf)
    }

    // Choose the minimum number of splits. If mapred.map.tasks is set, use that unless
    // it is smaller than what Spark suggests.
    val minSplits = math.max(hconf.getInt("mapred.map.tasks", 1), SharkEnv.sc.defaultMinSplits)
    val rdd = SharkEnv.sc.hadoopRDD(conf, ifc, classOf[Writable], classOf[Writable], minSplits)

    // Only take the value (skip the key) because Hive works only with values.
    rdd.map(_._2)
  }
}

object TableScanOperator {
  /** 
   * Deserializes partitions that have been loaded from Hive, presumably from
   * HDFS.
   */
  private class HiveDeserializingProcessor(
      private val tableDesc: TableDesc,
      private val hconf: SerializableHiveConf)
      extends PartitionProcessor {
    override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = {
      val deserializer = tableDesc.getDeserializerClass().newInstance()
      deserializer.initialize(hconf.value, tableDesc.getProperties)
      iter.map { value =>
        value match {
          case rowWithPart: Array[Object] => rowWithPart
          case v: Writable => deserializer.deserialize(v)
          case _ => throw new RuntimeException("Failed to match " + value.toString)
        }
      }
    }
  }
}
