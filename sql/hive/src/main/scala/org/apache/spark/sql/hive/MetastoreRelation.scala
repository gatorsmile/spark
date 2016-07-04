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

import com.google.common.base.Objects
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.execution.FileRelation

private[hive] case class MetastoreRelation(
    databaseName: String,
    tableName: String,
    alias: Option[String])
    (val catalogTable: CatalogTable,
     @transient private val sparkSession: SparkSession)
  extends LeafNode with MultiInstanceRelation with FileRelation with CatalogRelation {

  override def equals(other: Any): Boolean = other match {
    case relation: MetastoreRelation =>
      databaseName == relation.databaseName &&
        tableName == relation.tableName &&
        alias == relation.alias &&
        output == relation.output
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(databaseName, tableName, alias, output)
  }

  override protected def otherCopyArgs: Seq[AnyRef] = catalogTable :: sparkSession :: Nil

  @transient val hiveQlTable: HiveTable = HiveUtils.toHiveTable(catalogTable)

  @transient override lazy val statistics: Statistics = Statistics(
    sizeInBytes = HiveUtils.getHiveTableSizeInBytes(hiveQlTable, sparkSession)
  )

  /** Only compare database and tablename, not alias. */
  override def sameResult(plan: LogicalPlan): Boolean = {
    plan.canonicalized match {
      case mr: MetastoreRelation =>
        mr.databaseName == databaseName && mr.tableName == tableName
      case _ => false
    }
  }

  val tableDesc = new TableDesc(
    hiveQlTable.getInputFormatClass,
    // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
    // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
    // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
    // HiveSequenceFileOutputFormat.
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata
  )

  implicit class SchemaAttribute(f: CatalogColumn) {
    def toAttribute: AttributeReference = AttributeReference(
      f.name,
      CatalystSqlParser.parseDataType(f.dataType),
      // Since data can be dumped in randomly with no validation, everything is nullable.
      nullable = true
    )(qualifier = Some(alias.getOrElse(tableName)))
  }

  /** PartitionKey attributes */
  val partitionKeys = catalogTable.partitionColumns.map(_.toAttribute)

  /** Non-partitionKey attributes */
  // TODO: just make this hold the schema itself, not just non-partition columns
  val attributes = catalogTable.schema
    .filter { c => !catalogTable.partitionColumnNames.contains(c.name) }
    .map(_.toAttribute)

  val output = attributes ++ partitionKeys

  /** An attribute map that can be used to lookup original attributes based on expression id. */
  val attributeMap = AttributeMap(output.map(o => (o, o)))

  /** An attribute map for determining the ordinal for non-partition columns. */
  val columnOrdinals = AttributeMap(attributes.zipWithIndex)

  override def inputFiles: Array[String] = {
    val partLocations = sparkSession.sessionState.catalog.listPartitions(catalogTable.identifier)
      .flatMap(_.storage.locationUri).toArray
    if (partLocations.nonEmpty) {
      partLocations
    } else {
      Array(
        catalogTable.storage.locationUri.getOrElse(
          sys.error(s"Could not get the location of ${catalogTable.qualifiedName}.")))
    }
  }

  override def newInstance(): MetastoreRelation = {
    MetastoreRelation(databaseName, tableName, alias)(catalogTable, sparkSession)
  }
}
