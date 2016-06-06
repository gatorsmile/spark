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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.CreateDataSourceTableUtils

/**
 * A cache for interacting with the Hive metastore. Used for things like creating data source tables
 */
private[hive] class HiveMetastoreCatalog(sparkSession: SparkSession) extends Logging {
  private val sessionState = sparkSession.sessionState

  /** A fully qualified identifier for a table (i.e., database.tableName) */
  private case class QualifiedTableName(database: String, name: String)

  private def getQualifiedTableName(tableIdent: TableIdentifier): QualifiedTableName = {
    QualifiedTableName(
      tableIdent.database.getOrElse(sessionState.catalog.getCurrentDatabase).toLowerCase,
      tableIdent.table.toLowerCase)
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  private val cachedDataSourceTables: LoadingCache[QualifiedTableName, LogicalPlan] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
        val tableMetadata =
          sessionState.catalog.getTableMetadata(TableIdentifier(in.name, Some(in.database)))
        CreateDataSourceTableUtils.buildDataSourceTableForRead(sparkSession, tableMetadata)
      }
    }
    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  /**
   * Data Source Table is inserted directly, using Cache.put.
   * Note, this is not using automatic cache loading.
   */
  def cacheTable(tableIdent: TableIdentifier, plan: LogicalPlan): Unit = {
    cachedDataSourceTables.put(getQualifiedTableName(tableIdent), plan)
  }

  def getTableIfPresent(tableIdent: TableIdentifier): Option[LogicalPlan] = {
    cachedDataSourceTables.getIfPresent(getQualifiedTableName(tableIdent)) match {
      case null => None // Cache miss
      case o: LogicalPlan => Option(o.asInstanceOf[LogicalPlan])
    }
  }

  def getTable(tableIdent: TableIdentifier): LogicalPlan = {
    cachedDataSourceTables.get(getQualifiedTableName(tableIdent))
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

  def invalidateAll(): Unit = {
    cachedDataSourceTables.invalidateAll()
  }
}
