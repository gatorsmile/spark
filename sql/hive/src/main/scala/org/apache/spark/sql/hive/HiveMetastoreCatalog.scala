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

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.{CreateDataSourceTableUtils, DDLUtils}
import org.apache.spark.sql.execution.datasources.{Partition => _, _}

/** A fully qualified identifier for a table (i.e., database.tableName) */
case class QualifiedTableName(database: String, name: String)

/**
 * Legacy catalog for interacting with the Hive metastore.
 *
 * This is still used for things like creating data source tables, but in the future will be
 * cleaned up to integrate more nicely with [[HiveExternalCatalog]].
 */
private[hive] class HiveMetastoreCatalog(sparkSession: SparkSession) extends Logging {
  private val sessionState = sparkSession.sessionState.asInstanceOf[HiveSessionState]
  private val client = sparkSession.sharedState.asInstanceOf[HiveSharedState].metadataHive

  private def getCurrentDatabase: String = sessionState.catalog.getCurrentDatabase

  def getQualifiedTableName(tableIdent: TableIdentifier): QualifiedTableName = {
    QualifiedTableName(
      tableIdent.database.getOrElse(getCurrentDatabase).toLowerCase,
      tableIdent.table.toLowerCase)
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected[hive] val cachedDataSourceTables: LoadingCache[QualifiedTableName, LogicalPlan] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
        val table: CatalogTable = client.getTable(in.database, in.name)
        val dataSource = DDLUtils.getDataSourceTable(sparkSession, table)
        LogicalRelation(
          dataSource.resolveRelation(checkPathExist = true),
          metastoreTableIdentifier = Some(TableIdentifier(in.name, Some(in.database))))
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  def putTable(tableIdent: TableIdentifier, plan: LogicalPlan): Unit = {
    cachedDataSourceTables.put(getQualifiedTableName(tableIdent), plan)
  }

  def getRelationOption(tableIdent: TableIdentifier): Option[LogicalPlan] = {
    cachedDataSourceTables.getIfPresent(getQualifiedTableName(tableIdent)) match {
      case null => None // Cache miss
      case o: LogicalPlan => Option(o.asInstanceOf[LogicalPlan])
    }
  }

  def refreshTable(tableIdent: TableIdentifier): Unit = {
    // refreshTable does not eagerly reload the cache. It just invalidate the cache.
    // Next time when we use the table, it will be populated in the cache.
    // Since we also cache ParquetRelations converted from Hive Parquet tables and
    // adding converted ParquetRelations into the cache is not defined in the load function
    // of the cache (instead, we add the cache entry in convertToParquetRelation),
    // it is better at here to invalidate the cache to avoid confusing waring logs from the
    // cache loader (e.g. cannot find data source provider, which is only defined for
    // data source table.).
    invalidateTable(tableIdent)
  }

  def invalidateTable(tableIdent: TableIdentifier): Unit = {
    cachedDataSourceTables.invalidate(getQualifiedTableName(tableIdent))
  }

  def hiveDefaultTableFilePath(tableIdent: TableIdentifier): String = {
    // Code based on: hiveWarehouse.getTablePath(currentDatabase, tableName)
    val QualifiedTableName(dbName, tblName) = getQualifiedTableName(tableIdent)
    new Path(new Path(client.getDatabase(dbName).locationUri), tblName).toString
  }

  def lookupRelation(
      tableIdent: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    val qualifiedTableName = getQualifiedTableName(tableIdent)
    val table = client.getTable(qualifiedTableName.database, qualifiedTableName.name)

    if (table.properties.get(CreateDataSourceTableUtils.DATASOURCE_PROVIDER).isDefined) {
      val dataSourceTable = cachedDataSourceTables(qualifiedTableName)
      val qualifiedTable = SubqueryAlias(qualifiedTableName.name, dataSourceTable)
      // Then, if alias is specified, wrap the table with a Subquery using the alias.
      // Otherwise, wrap the table with a Subquery using the table name.
      alias.map(a => SubqueryAlias(a, qualifiedTable)).getOrElse(qualifiedTable)
    } else if (table.tableType == CatalogTableType.VIEW) {
      val viewText = table.viewText.getOrElse(sys.error("Invalid view without text."))
      alias match {
        // because hive use things like `_c0` to build the expanded text
        // currently we cannot support view from "create view v1(c1) as ..."
        case None =>
          SubqueryAlias(table.identifier.table, sessionState.sqlParser.parsePlan(viewText))
        case Some(aliasText) =>
          SubqueryAlias(aliasText, sessionState.sqlParser.parsePlan(viewText))
      }
    } else {
      MetastoreRelation(
        qualifiedTableName.database, qualifiedTableName.name, alias)(table, client, sparkSession)
    }
  }

}
