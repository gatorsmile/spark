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

import java.util.Properties

import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.{FileRelation, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.execution.{HiveInsertUtils, HiveRelationScanExec}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType


case class HiveRelation(
    sparkSession: SparkSession,
    catalogTable: CatalogTable,
    properties: Properties = new Properties())
  extends BaseRelation
    with PrunedFilteredHiveScan
    with InsertHiveRelation
    with FileRelation
    with Logging {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  @transient val hiveQlTable: HiveTable = HiveUtils.toHiveTable(catalogTable)

  val tableDesc = new TableDesc(
    hiveQlTable.getInputFormatClass,
    // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
    // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
    // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
    // HiveSequenceFileOutputFormat.
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata
  )

  /** PartitionKey attributes */
  override lazy val partitionKeys: Seq[String] = catalogTable.partitionColumns.map(_.name)

  override val schema = StructType.fromCatalogTable(catalogTable)

  override def buildScan(
      relation: LogicalRelation,
      requestedAttributes: Seq[Attribute],
      requestedPartitionAttributes: Seq[Attribute],
      partitionPruningPred: Seq[Expression]): SparkPlan = {
    // scalastyle:off println
    println("scan build is started")
    // scalastyle:on println

    val e = HiveRelationScanExec(
      catalogTable = catalogTable,
      requestedAttributes = requestedAttributes,
      requestedPartitionAttributes = requestedPartitionAttributes,
      partitionPruningPred = partitionPruningPred)(sparkSession)

    // scalastyle:off println
    println("scan build is done")
    // scalastyle:on println

    e
  }

  override def insert(
      partition: Map[String, Option[String]],
      data: DataFrame,
      overwrite: Boolean,
      ifNotExists: Boolean): Unit = {
    // scalastyle:off println
    println("insert is started")
    // scalastyle:on println

    HiveInsertUtils(sparkSession, this, partition, data, overwrite, ifNotExists).doExecute()

    // scalastyle:off println
    println("insert is done")
    // scalastyle:on println
  }

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

  override def sizeInBytes: Long =
    HiveUtils.getHiveTableSizeInBytes(hiveQlTable, sparkSession)
}
