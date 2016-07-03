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

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogRelation, CatalogTable}
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

  override def sizeInBytes: Long =
    HiveUtils.getHiveTableSizeInBytes(HiveUtils.toHiveTable(catalogTable), sparkSession)
}
