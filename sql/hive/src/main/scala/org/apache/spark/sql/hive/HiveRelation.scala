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

import scala.util.control.NonFatal

import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
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
    with HiveTableRelation
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

  override def createTableAsSelect(
      tableDesc: CatalogTable,
      query: LogicalPlan,
      ignoreIfExists: Boolean): Seq[Row] = {
    lazy val metastoreRelation: LogicalRelation = {
      import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
      import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
      import org.apache.hadoop.io.Text
      import org.apache.hadoop.mapred.TextInputFormat

      val withFormat =
        tableDesc.withNewStorage(
          inputFormat =
            tableDesc.storage.inputFormat.orElse(Some(classOf[TextInputFormat].getName)),
          outputFormat =
            tableDesc.storage.outputFormat
              .orElse(Some(classOf[HiveIgnoreKeyTextOutputFormat[Text, Text]].getName)),
          serde = tableDesc.storage.serde.orElse(Some(classOf[LazySimpleSerDe].getName)),
          compressed = tableDesc.storage.compressed)

      val withSchema = if (withFormat.schema.isEmpty) {
        // Hive doesn't support specifying the column list for target table in CTAS
        // However we don't think SparkSQL should follow that.
        tableDesc.copy(schema = query.output.map { c =>
          CatalogColumn(c.name, c.dataType.catalogString)
        })
      } else {
        withFormat
      }

      sparkSession.sessionState.catalog.createTable(withSchema, ignoreIfExists = false)

      // Get the Metastore Relation
      sparkSession.sessionState.catalog.lookupRelation(tableDesc.identifier) match {
        case r @ LogicalRelation(_: HiveRelation, _, _) => r
      }
    }
    // TODO ideally, we should get the output data ready first and then
    // add the relation into catalog, just in case of failure occurs while data
    // processing.
    if (sparkSession.sessionState.catalog.tableExists(tableDesc.identifier)) {
      if (ignoreIfExists) {
        // table already exists, will do nothing, to keep consistent with Hive
      } else {
        throw new AnalysisException(s"${tableDesc.identifier} already exists.")
      }
    } else {
      try {
        sparkSession.sessionState.executePlan(InsertIntoTable(
          metastoreRelation, Map(), query, overwrite = true, ifNotExists = false)).toRdd
      } catch {
        case NonFatal(e) =>
          // drop the created table.
          sparkSession.sessionState.catalog.dropTable(
            tableDesc.identifier,
            ignoreIfNotExists = true)
          throw e
      }
    }

    Seq.empty[Row]
  }

}
