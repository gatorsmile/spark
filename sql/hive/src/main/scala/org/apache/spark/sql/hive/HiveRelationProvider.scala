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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class HiveRelationProvider extends RelationProvider with DataSourceRegister {

  override def shortName(): String = "hive"

  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val hiveOptions = new HiveOptions(parameters)
    val tableIdent = sqlContext.sessionState.sqlParser.parseTableIdentifier(hiveOptions.table)
    val catalogTable = sqlContext.sessionState.catalog.getTableMetadata(tableIdent)

    val properties = new Properties()
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    HiveRelation(sqlContext.sparkSession, catalogTable, properties)
  }
}
