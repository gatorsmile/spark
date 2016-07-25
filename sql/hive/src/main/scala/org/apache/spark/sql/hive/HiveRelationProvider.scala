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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{CreateHiveTableRelationAsSelectProvider, DataSourceRegister}

class HiveRelationProvider
  extends DataSourceRegister with CreateHiveTableRelationAsSelectProvider {

  override def shortName(): String = "hive"

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      tableDesc: CatalogTable,
      parameters: Map[String, String],
      query: LogicalPlan): Unit = {
    // val hiveOptions = new HiveOptions(parameters)

    val properties = new Properties()
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    HiveRelation(sqlContext.sparkSession, tableDesc, properties)
      .createTableAsSelect(tableDesc, query, mode == SaveMode.Ignore)
  }

}
