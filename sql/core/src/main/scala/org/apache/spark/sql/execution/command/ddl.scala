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

package org.apache.spark.sql.execution.command

import scala.collection.{GenMap, GenSeq}
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, Resolver}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BinaryComparison}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, PredicateHelper}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

// Note: The definition of these commands are based on the ones described in
// https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL

/**
 * A command for users to create a new database.
 *
 * It will issue an error message when the database with the same name already exists,
 * unless 'ifNotExists' is true.
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE (DATABASE|SCHEMA) [IF NOT EXISTS] database_name
 *     [COMMENT database_comment]
 *     [LOCATION database_directory]
 *     [WITH DBPROPERTIES (property_name=property_value, ...)];
 * }}}
 */
case class CreateDatabaseCommand(
    databaseName: String,
    ifNotExists: Boolean,
    path: Option[String],
    comment: Option[String],
    props: Map[String, String])
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    catalog.createDatabase(
      CatalogDatabase(
        databaseName,
        comment.getOrElse(""),
        path.getOrElse(catalog.getDefaultDBPath(databaseName)),
        props),
      ifNotExists)
    Seq.empty[Row]
  }
}


/**
 * A command for users to remove a database from the system.
 *
 * 'ifExists':
 * - true, if database_name does't exist, no action
 * - false (default), if database_name does't exist, a warning message will be issued
 * 'cascade':
 * - true, the dependent objects are automatically dropped before dropping database.
 * - false (default), it is in the Restrict mode. The database cannot be dropped if
 * it is not empty. The inclusive tables must be dropped at first.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *    DROP DATABASE [IF EXISTS] database_name [RESTRICT|CASCADE];
 * }}}
 */
case class DropDatabaseCommand(
    databaseName: String,
    ifExists: Boolean,
    cascade: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sessionState.catalog.dropDatabase(databaseName, ifExists, cascade)
    Seq.empty[Row]
  }
}

/**
 * A command for users to add new (key, value) pairs into DBPROPERTIES
 * If the database does not exist, an error message will be issued to indicate the database
 * does not exist.
 * The syntax of using this command in SQL is:
 * {{{
 *    ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...)
 * }}}
 */
case class AlterDatabasePropertiesCommand(
    databaseName: String,
    props: Map[String, String])
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val db: CatalogDatabase = catalog.getDatabaseMetadata(databaseName)
    catalog.alterDatabase(db.copy(properties = db.properties ++ props))

    Seq.empty[Row]
  }
}

/**
 * A command for users to show the name of the database, its comment (if one has been set), and its
 * root location on the filesystem. When extended is true, it also shows the database's properties
 * If the database does not exist, an error message will be issued to indicate the database
 * does not exist.
 * The syntax of using this command in SQL is
 * {{{
 *    DESCRIBE DATABASE [EXTENDED] db_name
 * }}}
 */
case class DescribeDatabaseCommand(
    databaseName: String,
    extended: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dbMetadata: CatalogDatabase =
      sparkSession.sessionState.catalog.getDatabaseMetadata(databaseName)
    val result =
      Row("Database Name", dbMetadata.name) ::
        Row("Description", dbMetadata.description) ::
        Row("Location", dbMetadata.locationUri) :: Nil

    if (extended) {
      val properties =
        if (dbMetadata.properties.isEmpty) {
          ""
        } else {
          dbMetadata.properties.toSeq.mkString("(", ", ", ")")
        }
      result :+ Row("Properties", properties)
    } else {
      result
    }
  }

  override val output: Seq[Attribute] = {
    AttributeReference("database_description_item", StringType, nullable = false)() ::
      AttributeReference("database_description_value", StringType, nullable = false)() :: Nil
  }
}

/**
 * Drops a table/view from the metastore and removes it if it is cached.
 *
 * The syntax of this command is:
 * {{{
 *   DROP TABLE [IF EXISTS] table_name;
 *   DROP VIEW [IF EXISTS] [db_name.]view_name;
 * }}}
 */
case class DropTableCommand(
    tableName: TableIdentifier,
    ifExists: Boolean,
    isView: Boolean,
    purge: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog

    if (!catalog.isTemporaryTable(tableName) && catalog.tableExists(tableName)) {
      // If the command DROP VIEW is to drop a table or DROP TABLE is to drop a view
      // issue an exception.
      catalog.getTableMetadata(tableName).tableType match {
        case CatalogTableType.VIEW if !isView =>
          throw new AnalysisException(
            "Cannot drop a view with DROP TABLE. Please use DROP VIEW instead")
        case o if o != CatalogTableType.VIEW && isView =>
          throw new AnalysisException(
            s"Cannot drop a table with DROP VIEW. Please use DROP TABLE instead")
        case _ =>
      }
    }
    try {
      sparkSession.sharedState.cacheManager.uncacheQuery(
        sparkSession.table(tableName.quotedString))
    } catch {
      case _: NoSuchTableException if ifExists =>
      case NonFatal(e) => log.warn(e.toString, e)
    }
    catalog.refreshTable(tableName)
    catalog.dropTable(tableName, ifExists, purge)
    Seq.empty[Row]
  }
}

/**
 * A command that sets table/view properties.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
 *   ALTER VIEW view1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
 * }}}
 */
case class AlterTableSetPropertiesCommand(
    tableName: TableIdentifier,
    properties: Map[String, String],
    isView: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView)
    // This overrides old properties
    val newTable = table.copy(properties = table.properties ++ properties)
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }

}

/**
 * A command that unsets table/view properties.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
 *   ALTER VIEW view1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
 * }}}
 */
case class AlterTableUnsetPropertiesCommand(
    tableName: TableIdentifier,
    propKeys: Seq[String],
    ifExists: Boolean,
    isView: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView)
    if (!ifExists) {
      propKeys.foreach { k =>
        if (!table.properties.contains(k)) {
          throw new AnalysisException(
            s"Attempted to unset non-existent property '$k' in table '${table.identifier}'")
        }
      }
    }
    val newProperties = table.properties.filter { case (k, _) => !propKeys.contains(k) }
    val newTable = table.copy(properties = newProperties)
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }

}

/**
 * A command that sets the serde class and/or serde properties of a table/view.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table [PARTITION spec] SET SERDE serde_name [WITH SERDEPROPERTIES props];
 *   ALTER TABLE table [PARTITION spec] SET SERDEPROPERTIES serde_properties;
 * }}}
 */
case class AlterTableSerDePropertiesCommand(
    tableName: TableIdentifier,
    serdeClassName: Option[String],
    serdeProperties: Option[Map[String, String]],
    partSpec: Option[TablePartitionSpec])
  extends RunnableCommand {

  // should never happen if we parsed things correctly
  require(serdeClassName.isDefined || serdeProperties.isDefined,
    "ALTER TABLE attempted to set neither serde class name nor serde properties")

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    // For datasource tables, disallow setting serde or specifying partition
    if (partSpec.isDefined && DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException("Operation not allowed: ALTER TABLE SET " +
        "[SERDE | SERDEPROPERTIES] for a specific partition is not supported " +
        "for tables created with the datasource API")
    }
    if (serdeClassName.isDefined && DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException("Operation not allowed: ALTER TABLE SET SERDE is " +
        "not supported for tables created with the datasource API")
    }
    if (partSpec.isEmpty) {
      val newTable = table.withNewStorage(
        serde = serdeClassName.orElse(table.storage.serde),
        properties = table.storage.properties ++ serdeProperties.getOrElse(Map()))
      catalog.alterTable(newTable)
    } else {
      val spec = partSpec.get
      val part = catalog.getPartition(table.identifier, spec)
      val newPart = part.copy(storage = part.storage.copy(
        serde = serdeClassName.orElse(part.storage.serde),
        properties = part.storage.properties ++ serdeProperties.getOrElse(Map())))
      catalog.alterPartitions(table.identifier, Seq(newPart))
    }
    Seq.empty[Row]
  }

}

/**
 * Add Partition in ALTER TABLE: add the table partitions.
 *
 * 'partitionSpecsAndLocs': the syntax of ALTER VIEW is identical to ALTER TABLE,
 * EXCEPT that it is ILLEGAL to specify a LOCATION clause.
 * An error message will be issued if the partition exists, unless 'ifNotExists' is true.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table ADD [IF NOT EXISTS] PARTITION spec [LOCATION 'loc1']
 * }}}
 */
case class AlterTableAddPartitionCommand(
    tableName: TableIdentifier,
    partitionSpecsAndLocs: Seq[(TablePartitionSpec, Option[String])],
    ifNotExists: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "ALTER TABLE ADD PARTITION")
    val parts = partitionSpecsAndLocs.map { case (spec, location) =>
      val normalizedSpec = PartitioningUtils.normalizePartitionSpec(
        spec,
        table.partitionColumnNames,
        table.identifier.quotedString,
        sparkSession.sessionState.conf.resolver)
      // inherit table storage format (possibly except for location)
      CatalogTablePartition(normalizedSpec, table.storage.copy(locationUri = location))
    }
    catalog.createPartitions(table.identifier, parts, ignoreIfExists = ifNotExists)
    Seq.empty[Row]
  }

}

/**
 * Alter a table partition's spec.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table PARTITION spec1 RENAME TO PARTITION spec2;
 * }}}
 */
case class AlterTableRenamePartitionCommand(
    tableName: TableIdentifier,
    oldPartition: TablePartitionSpec,
    newPartition: TablePartitionSpec)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "ALTER TABLE RENAME PARTITION")

    val normalizedOldPartition = PartitioningUtils.normalizePartitionSpec(
      oldPartition,
      table.partitionColumnNames,
      table.identifier.quotedString,
      sparkSession.sessionState.conf.resolver)

    val normalizedNewPartition = PartitioningUtils.normalizePartitionSpec(
      newPartition,
      table.partitionColumnNames,
      table.identifier.quotedString,
      sparkSession.sessionState.conf.resolver)

    catalog.renamePartitions(
      tableName, Seq(normalizedOldPartition), Seq(normalizedNewPartition))
    Seq.empty[Row]
  }

}

/**
 * Drop Partition in ALTER TABLE: to drop a particular partition for a table.
 *
 * This removes the data and metadata for this partition.
 * The data is actually moved to the .Trash/Current directory if Trash is configured,
 * unless 'purge' is true, but the metadata is completely lost.
 * An error message will be issued if the partition does not exist, unless 'ifExists' is true.
 * Note: purge is always false when the target is a view.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...] [PURGE];
 * }}}
 */
case class AlterTableDropPartitionCommand(
    tableName: TableIdentifier,
    specs: Seq[Expression],
    ifExists: Boolean,
    purge: Boolean)
  extends RunnableCommand with PredicateHelper {

  private def isRangeComparison(expr: Expression): Boolean = {
    expr.find(e => e.isInstanceOf[BinaryComparison] && !e.isInstanceOf[EqualTo]).isDefined
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val resolver = sparkSession.sessionState.conf.resolver
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    DDLUtils.verifyPartitionProviderIsHive(sparkSession, table, "ALTER TABLE DROP PARTITION")

    specs.foreach { expr =>
      expr.references.foreach { attr =>
        if (!table.partitionColumnNames.exists(resolver(_, attr.name))) {
          throw new AnalysisException(s"${attr.name} is not a valid partition column " +
            s"in table ${table.identifier.quotedString}.")
        }
      }
    }

    if (specs.exists(isRangeComparison)) {
      val partitionSet = specs.flatMap { spec =>
        val partitions = catalog.listPartitionsByFilter(table.identifier, Seq(spec)).map(_.spec)
        if (partitions.isEmpty && !ifExists) {
          throw new AnalysisException(s"There is no partition for ${spec.sql}")
        }
        partitions
      }.distinct
      catalog.dropPartitions(
        table.identifier, partitionSet, ignoreIfNotExists = ifExists, purge = purge)
    } else {
      val normalizedSpecs = specs.map { expr =>
        val spec = splitConjunctivePredicates(expr).map {
          case BinaryComparison(AttributeReference(name, _, _, _), right) => name -> right.toString
        }.toMap
        PartitioningUtils.normalizePartitionSpec(
          spec,
          table.partitionColumnNames,
          table.identifier.quotedString,
          resolver)
      }
      catalog.dropPartitions(
        table.identifier, normalizedSpecs, ignoreIfNotExists = ifExists, purge = purge)
    }
    Seq.empty[Row]
  }

}


case class PartitionStatistics(numFiles: Int, totalSize: Long)

/**
 * Recover Partitions in ALTER TABLE: recover all the partition in the directory of a table and
 * update the catalog.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table RECOVER PARTITIONS;
 *   MSCK REPAIR TABLE table;
 * }}}
 */
case class AlterTableRecoverPartitionsCommand(
    tableName: TableIdentifier,
    cmd: String = "ALTER TABLE RECOVER PARTITIONS") extends RunnableCommand {

  // These are list of statistics that can be collected quickly without requiring a scan of the data
  // see https://github.com/apache/hive/blob/master/
  //   common/src/java/org/apache/hadoop/hive/common/StatsSetupConst.java
  val NUM_FILES = "numFiles"
  val TOTAL_SIZE = "totalSize"
  val DDL_TIME = "transient_lastDdlTime"

  private def getPathFilter(hadoopConf: Configuration): PathFilter = {
    // Dummy jobconf to get to the pathFilter defined in configuration
    // It's very expensive to create a JobConf(ClassUtil.findContainingJar() is slow)
    val jobConf = new JobConf(hadoopConf, this.getClass)
    val pathFilter = FileInputFormat.getInputPathFilter(jobConf)
    new PathFilter {
      override def accept(path: Path): Boolean = {
        val name = path.getName
        if (name != "_SUCCESS" && name != "_temporary" && !name.startsWith(".")) {
          pathFilter == null || pathFilter.accept(path)
        } else {
          false
        }
      }
    }
  }

  override def run(spark: SparkSession): Seq[Row] = {
    val catalog = spark.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val tableIdentWithDB = table.identifier.quotedString
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    if (table.partitionColumnNames.isEmpty) {
      throw new AnalysisException(
        s"Operation not allowed: $cmd only works on partitioned tables: $tableIdentWithDB")
    }

    if (table.storage.locationUri.isEmpty) {
      throw new AnalysisException(s"Operation not allowed: $cmd only works on table with " +
        s"location provided: $tableIdentWithDB")
    }

    val root = new Path(table.location)
    logInfo(s"Recover all the partitions in $root")
    val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)

    val threshold = spark.conf.get("spark.rdd.parallelListingThreshold", "10").toInt
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val pathFilter = getPathFilter(hadoopConf)
    val partitionSpecsAndLocs = scanPartitions(spark, fs, pathFilter, root, Map(),
      table.partitionColumnNames, threshold, spark.sessionState.conf.resolver)
    val total = partitionSpecsAndLocs.length
    logInfo(s"Found $total partitions in $root")

    val partitionStats = if (spark.sqlContext.conf.gatherFastStats) {
      gatherPartitionStats(spark, partitionSpecsAndLocs, fs, pathFilter, threshold)
    } else {
      GenMap.empty[String, PartitionStatistics]
    }
    logInfo(s"Finished to gather the fast stats for all $total partitions.")

    addPartitions(spark, table, partitionSpecsAndLocs, partitionStats)
    // Updates the table to indicate that its partition metadata is stored in the Hive metastore.
    // This is always the case for Hive format tables, but is not true for Datasource tables created
    // before Spark 2.1 unless they are converted via `msck repair table`.
    spark.sessionState.catalog.alterTable(table.copy(tracksPartitionsInCatalog = true))
    catalog.refreshTable(tableName)
    logInfo(s"Recovered all partitions ($total).")
    Seq.empty[Row]
  }

  @transient private lazy val evalTaskSupport = new ForkJoinTaskSupport(new ForkJoinPool(8))

  private def scanPartitions(
      spark: SparkSession,
      fs: FileSystem,
      filter: PathFilter,
      path: Path,
      spec: TablePartitionSpec,
      partitionNames: Seq[String],
      threshold: Int,
      resolver: Resolver): GenSeq[(TablePartitionSpec, Path)] = {
    if (partitionNames.isEmpty) {
      return Seq(spec -> path)
    }

    val statuses = fs.listStatus(path, filter)
    val statusPar: GenSeq[FileStatus] =
      if (partitionNames.length > 1 && statuses.length > threshold || partitionNames.length > 2) {
        // parallelize the list of partitions here, then we can have better parallelism later.
        val parArray = statuses.par
        parArray.tasksupport = evalTaskSupport
        parArray
      } else {
        statuses
      }
    statusPar.flatMap { st =>
      val name = st.getPath.getName
      if (st.isDirectory && name.contains("=")) {
        val ps = name.split("=", 2)
        val columnName = ExternalCatalogUtils.unescapePathName(ps(0))
        // TODO: Validate the value
        val value = ExternalCatalogUtils.unescapePathName(ps(1))
        if (resolver(columnName, partitionNames.head)) {
          scanPartitions(spark, fs, filter, st.getPath, spec ++ Map(partitionNames.head -> value),
            partitionNames.drop(1), threshold, resolver)
        } else {
          logWarning(
            s"expected partition column ${partitionNames.head}, but got ${ps(0)}, ignoring it")
          Seq()
        }
      } else {
        logWarning(s"ignore ${new Path(path, name)}")
        Seq()
      }
    }
  }

  private def gatherPartitionStats(
      spark: SparkSession,
      partitionSpecsAndLocs: GenSeq[(TablePartitionSpec, Path)],
      fs: FileSystem,
      pathFilter: PathFilter,
      threshold: Int): GenMap[String, PartitionStatistics] = {
    if (partitionSpecsAndLocs.length > threshold) {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val serializableConfiguration = new SerializableConfiguration(hadoopConf)
      val serializedPaths = partitionSpecsAndLocs.map(_._2.toString).toArray

      // Set the number of parallelism to prevent following file listing from generating many tasks
      // in case of large #defaultParallelism.
      val numParallelism = Math.min(serializedPaths.length,
        Math.min(spark.sparkContext.defaultParallelism, 10000))
      // gather the fast stats for all the partitions otherwise Hive metastore will list all the
      // files for all the new partitions in sequential way, which is super slow.
      logInfo(s"Gather the fast stats in parallel using $numParallelism tasks.")
      spark.sparkContext.parallelize(serializedPaths, numParallelism)
        .mapPartitions { paths =>
          val pathFilter = getPathFilter(serializableConfiguration.value)
          paths.map(new Path(_)).map{ path =>
            val fs = path.getFileSystem(serializableConfiguration.value)
            val statuses = fs.listStatus(path, pathFilter)
            (path.toString, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
          }
        }.collectAsMap()
    } else {
      partitionSpecsAndLocs.map { case (_, location) =>
        val statuses = fs.listStatus(location, pathFilter)
        (location.toString, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
      }.toMap
    }
  }

  private def addPartitions(
      spark: SparkSession,
      table: CatalogTable,
      partitionSpecsAndLocs: GenSeq[(TablePartitionSpec, Path)],
      partitionStats: GenMap[String, PartitionStatistics]): Unit = {
    val total = partitionSpecsAndLocs.length
    var done = 0L
    // Hive metastore may not have enough memory to handle millions of partitions in single RPC,
    // we should split them into smaller batches. Since Hive client is not thread safe, we cannot
    // do this in parallel.
    val batchSize = 100
    partitionSpecsAndLocs.toIterator.grouped(batchSize).foreach { batch =>
      val now = System.currentTimeMillis() / 1000
      val parts = batch.map { case (spec, location) =>
        val params = partitionStats.get(location.toString).map {
          case PartitionStatistics(numFiles, totalSize) =>
            // This two fast stat could prevent Hive metastore to list the files again.
            Map(NUM_FILES -> numFiles.toString,
              TOTAL_SIZE -> totalSize.toString,
              // Workaround a bug in HiveMetastore that try to mutate a read-only parameters.
              // see metastore/src/java/org/apache/hadoop/hive/metastore/HiveMetaStore.java
              DDL_TIME -> now.toString)
        }.getOrElse(Map.empty)
        // inherit table storage format (possibly except for location)
        CatalogTablePartition(
          spec,
          table.storage.copy(locationUri = Some(location.toUri.toString)),
          params)
      }
      spark.sessionState.catalog.createPartitions(tableName, parts, ignoreIfExists = true)
      done += parts.length
      logDebug(s"Recovered ${parts.length} partitions ($done/$total so far)")
    }
  }
}


/**
 * A command that sets the location of a table or a partition.
 *
 * For normal tables, this just sets the location URI in the table/partition's storage format.
 * For datasource tables, this sets a "path" parameter in the table/partition's serde properties.
 *
 * The syntax of this command is:
 * {{{
 *    ALTER TABLE table_name [PARTITION partition_spec] SET LOCATION "loc";
 * }}}
 */
case class AlterTableSetLocationCommand(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec],
    location: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    DDLUtils.verifyAlterTableType(catalog, table, isView = false)
    partitionSpec match {
      case Some(spec) =>
        DDLUtils.verifyPartitionProviderIsHive(
          sparkSession, table, "ALTER TABLE ... SET LOCATION")
        // Partition spec is specified, so we set the location only for this partition
        val part = catalog.getPartition(table.identifier, spec)
        val newPart = part.copy(storage = part.storage.copy(locationUri = Some(location)))
        catalog.alterPartitions(table.identifier, Seq(newPart))
      case None =>
        // No partition spec is specified, so we set the location for the table itself
        catalog.alterTable(table.withNewStorage(locationUri = Some(location)))
    }
    Seq.empty[Row]
  }
}


object DDLUtils {
  val HIVE_PROVIDER = "hive"

  def isDatasourceTable(table: CatalogTable): Boolean = {
    table.provider.isDefined && table.provider.get != HIVE_PROVIDER
  }

  /**
   * Throws a standard error for actions that require partitionProvider = hive.
   */
  def verifyPartitionProviderIsHive(
      spark: SparkSession, table: CatalogTable, action: String): Unit = {
    val tableName = table.identifier.table
    if (!spark.sqlContext.conf.manageFilesourcePartitions && isDatasourceTable(table)) {
      throw new AnalysisException(
        s"$action is not allowed on $tableName since filesource partition management is " +
          "disabled (spark.sql.hive.manageFilesourcePartitions = false).")
    }
    if (!table.tracksPartitionsInCatalog && isDatasourceTable(table)) {
      throw new AnalysisException(
        s"$action is not allowed on $tableName since its partition metadata is not stored in " +
          "the Hive metastore. To import this information into the metastore, run " +
          s"`msck repair table $tableName`")
    }
  }

  /**
   * If the command ALTER VIEW is to alter a table or ALTER TABLE is to alter a view,
   * issue an exception [[AnalysisException]].
   *
   * Note: temporary views can be altered by both ALTER VIEW and ALTER TABLE commands,
   * since temporary views can be also created by CREATE TEMPORARY TABLE. In the future,
   * when we decided to drop the support, we should disallow users to alter temporary views
   * by ALTER TABLE.
   */
  def verifyAlterTableType(
      catalog: SessionCatalog,
      tableMetadata: CatalogTable,
      isView: Boolean): Unit = {
    if (!catalog.isTemporaryTable(tableMetadata.identifier)) {
      tableMetadata.tableType match {
        case CatalogTableType.VIEW if !isView =>
          throw new AnalysisException(
            "Cannot alter a view with ALTER TABLE. Please use ALTER VIEW instead")
        case o if o != CatalogTableType.VIEW && isView =>
          throw new AnalysisException(
            s"Cannot alter a table with ALTER VIEW. Please use ALTER TABLE instead")
        case _ =>
      }
    }
  }

}
