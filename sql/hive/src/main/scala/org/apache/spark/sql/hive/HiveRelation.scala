/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import java.io.IOException
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.hadoop.hive.metastore.{TableType => HiveTableType}
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogRelation, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType


case class HiveRelation(
    sparkSession: SparkSession,
    catalogTable: CatalogTable,
    properties: Properties = new Properties())
  extends BaseRelation
    with PrunedFilteredScan
    // with CreatableRelationProvider
    with InsertableRelation
    with FileRelation
    with CatalogRelation
    with Logging {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  implicit class SchemaAttribute(f: CatalogColumn) {
    def toAttribute: AttributeReference = {
      val alias: Option[String] =
        if (!properties.contains("alias")) {
          None
        } else {
          Some(properties.get("alias").toString)
        }
      AttributeReference(
        f.name,
        CatalystSqlParser.parseDataType(f.dataType),
        // Since data can be dumped in randomly with no validation, everything is nullable.
        nullable = true
      )(qualifier = Some(alias.getOrElse(catalogTable.identifier.table)))
    }
  }

  /** PartitionKey attributes */
  val partitionKeys = catalogTable.partitionColumns.map(_.toAttribute)

  /** Non-partitionKey attributes */
  // TODO: just make this hold the schema itself, not just non-partition columns
  val attributes = catalogTable.schema
    .filter { c => !catalogTable.partitionColumnNames.contains(c.name) }
    .map(_.toAttribute)

  override val output = attributes ++ partitionKeys

  override val schema = StructType.fromAttributes(output)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    sparkSession.sparkContext.parallelize(0 to 10).map(Row(_))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    // HiveInsertUtils.insert(data, overwrite)
    // data.queryExecution.toRdd
    // data.write
    //   .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
    //   .hive(catalogTable.identifier.unquotedString, properties)
  }

  // override def createRelation(
  //     sqlContext: SQLContext,
  //     mode: SaveMode,
  //     parameters: Map[String, String],
  //     data: DataFrame): BaseRelation = {
  // }

  override def inputFiles: Array[String] = {
    val partLocations = sparkSession.sessionState.catalog.listPartitions(catalogTable.identifier)
      .flatMap(_.storage.locationUri)
      .toArray
    if (partLocations.nonEmpty) {
      partLocations
    } else {
      Array(
        catalogTable.storage.locationUri.getOrElse(
          sys.error(s"Could not get the location of ${catalogTable.qualifiedName}.")))
    }
  }

  private def toHiveColumn(c: CatalogColumn): FieldSchema = {
    new FieldSchema(c.name, c.dataType, c.comment.orNull)
  }

  // TODO: merge this with HiveClientImpl#toHiveTable
  @transient val hiveQlTable: HiveTable = {
    // We start by constructing an API table as Hive performs several important transformations
    // internally when converting an API table to a QL table.
    val tTable = new org.apache.hadoop.hive.metastore.api.Table()
    tTable.setTableName(catalogTable.identifier.table)
    tTable.setDbName(catalogTable.database)

    val tableParameters = new java.util.HashMap[String, String]()
    tTable.setParameters(tableParameters)
    catalogTable.properties.foreach { case (k, v) => tableParameters.put(k, v) }

    tTable.setTableType(catalogTable.tableType match {
      case CatalogTableType.EXTERNAL => HiveTableType.EXTERNAL_TABLE.toString
      case CatalogTableType.MANAGED => HiveTableType.MANAGED_TABLE.toString
      case CatalogTableType.INDEX => HiveTableType.INDEX_TABLE.toString
      case CatalogTableType.VIEW => HiveTableType.VIRTUAL_VIEW.toString
    })

    val sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor()
    tTable.setSd(sd)

    // Note: In Hive the schema and partition columns must be disjoint sets
    val (partCols, schema) = catalogTable.schema.map(toHiveColumn).partition { c =>
      catalogTable.partitionColumnNames.contains(c.getName)
    }
    sd.setCols(schema.asJava)
    tTable.setPartitionKeys(partCols.asJava)

    catalogTable.storage.locationUri.foreach(sd.setLocation)
    catalogTable.storage.inputFormat.foreach(sd.setInputFormat)
    catalogTable.storage.outputFormat.foreach(sd.setOutputFormat)

    val serdeInfo = new org.apache.hadoop.hive.metastore.api.SerDeInfo
    catalogTable.storage.serde.foreach(serdeInfo.setSerializationLib)
    sd.setSerdeInfo(serdeInfo)

    val serdeParameters = new java.util.HashMap[String, String]()
    catalogTable.storage.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) }
    serdeInfo.setParameters(serdeParameters)

    new HiveTable(tTable)
  }

  override def sizeInBytes: Long = {
    val totalSize = hiveQlTable.getParameters.get(StatsSetupConst.TOTAL_SIZE)
    val rawDataSize = hiveQlTable.getParameters.get(StatsSetupConst.RAW_DATA_SIZE)
    // TODO: check if this estimate is valid for tables after partition pruning.
    // NOTE: getting `totalSize` directly from params is kind of hacky, but this should be
    // relatively cheap if parameters for the table are populated into the metastore.
    // Besides `totalSize`, there are also `numFiles`, `numRows`, `rawDataSize` keys
    // (see StatsSetupConst in Hive) that we can look at in the future.

    // When table is external,`totalSize` is always zero, which will influence join strategy
    // so when `totalSize` is zero, use `rawDataSize` instead
    // if the size is still less than zero, we try to get the file size from HDFS.
    // given this is only needed for optimization, if the HDFS call fails we return the default.
    if (totalSize != null && totalSize.toLong > 0L) {
      totalSize.toLong
    } else if (rawDataSize != null && rawDataSize.toLong > 0) {
      rawDataSize.toLong
    } else if (sparkSession.sessionState.conf.fallBackToHdfsForStatsEnabled) {
      try {
        val hadoopConf = sparkSession.sessionState.newHadoopConf()
        val fs: FileSystem = hiveQlTable.getPath.getFileSystem(hadoopConf)
        fs.getContentSummary(hiveQlTable.getPath).getLength
      } catch {
        case e: IOException =>
          logWarning("Failed to get table size from hdfs.", e)
          sparkSession.sessionState.conf.defaultSizeInBytes
      }
    } else {
      sparkSession.sessionState.conf.defaultSizeInBytes
    }
  }

}
