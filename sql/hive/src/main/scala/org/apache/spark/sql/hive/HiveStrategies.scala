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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.CreateHiveTableAsSelectLogicalPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.types.StructType

private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object Scripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ScriptTransformation(input, script, output, child, ioschema) =>
        val hiveIoSchema = HiveScriptIOSchema(ioschema)
        ScriptTransformation(input, script, output, planLater(child), hiveIoSchema) :: Nil
      case _ => Nil
    }
  }

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.InsertIntoTable(
          table: MetastoreRelation, partition, child, overwrite, ifNotExists) =>
        execution.InsertIntoHiveTable(
          table, partition, planLater(child), overwrite, ifNotExists) :: Nil
      case hive.InsertIntoHiveTable(
          table: MetastoreRelation, partition, child, overwrite, ifNotExists) =>
        execution.InsertIntoHiveTable(
          table, partition, planLater(child), overwrite, ifNotExists) :: Nil
      case _ => Nil
    }
  }

  /**
   * Retrieves data using a HiveTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: MetastoreRelation) =>
        // Filter out all predicates that only deal with partition keys, these are given to the
        // hive table scan operator to be used for partition pruning.
        val partitionKeyIds = AttributeSet(relation.partitionKeys)
        val (pruningPredicates, otherPredicates) = predicates.partition { predicate =>
          !predicate.references.isEmpty &&
          predicate.references.subsetOf(partitionKeyIds)
        }

        pruneFilterProject(
          projectList,
          otherPredicates,
          identity[Seq[Expression]],
          HiveTableScanExec(_, relation, pruningPredicates)(sparkSession)) :: Nil
      case _ =>
        Nil
    }
  }
}

/**
 * Creates any tables required for query execution.
 * For example, because of a CREATE TABLE X AS statement.
 */
private[hive] class CreateTables(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Wait until children are resolved.
    case p: LogicalPlan if !p.childrenResolved => p
    case p: LogicalPlan if p.resolved => p

    case p @ CreateHiveTableAsSelectLogicalPlan(table, child, allowExisting) =>
      val desc = if (table.storage.serde.isEmpty) {
        // add default serde
        table.withNewStorage(
          serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
      } else {
        table
      }

      val catalog = sparkSession.sessionState.catalog
      val db = table.identifier.database.getOrElse(catalog.getCurrentDatabase).toLowerCase

      execution.CreateHiveTableAsSelectCommand(
        desc.copy(identifier = TableIdentifier(table.identifier.table, Some(db))),
        child,
        allowExisting)
  }
}

/**
 * Casts input data to correct data types according to table definition before inserting into
 * that table.
 */
private[hive] object PreInsertionCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    // Wait until children are resolved.
    case p: LogicalPlan if !p.childrenResolved => p

    case p @ InsertIntoTable(table: MetastoreRelation, _, child, _, _) =>
      castChildOutput(p, table, child)
  }

  def castChildOutput(p: InsertIntoTable, table: MetastoreRelation, child: LogicalPlan)
  : LogicalPlan = {
    val childOutputDataTypes = child.output.map(_.dataType)
    val numDynamicPartitions = p.partition.values.count(_.isEmpty)
    val tableOutputDataTypes =
      (table.attributes ++ table.partitionKeys.takeRight(numDynamicPartitions))
        .take(child.output.length).map(_.dataType)

    if (childOutputDataTypes == tableOutputDataTypes) {
      hive.InsertIntoHiveTable(table, p.partition, p.child, p.overwrite, p.ifNotExists)
    } else if (childOutputDataTypes.size == tableOutputDataTypes.size &&
      childOutputDataTypes.zip(tableOutputDataTypes)
        .forall { case (left, right) => left.sameType(right) }) {
      // If both types ignoring nullability of ArrayType, MapType, StructType are the same,
      // use InsertIntoHiveTable instead of InsertIntoTable.
      hive.InsertIntoHiveTable(table, p.partition, p.child, p.overwrite, p.ifNotExists)
    } else {
      // Only do the casting when child output data types differ from table output data types.
      val castedChildOutput = child.output.zip(table.output).map {
        case (input, output) if input.dataType != output.dataType =>
          Alias(Cast(input, output.dataType), input.name)()
        case (input, _) => input
      }

      p.copy(child = logical.Project(castedChildOutput, child))
    }
  }
}

/**
 * When scanning or writing to non-partitioned Metastore Parquet tables, convert them to Parquet
 * data source relations for better performance.
 *
 * When scanning Metastore ORC tables, convert them to ORC data source relations
 * for better performance.
 */
class ConvertMetastoreTables(sparkSession: SparkSession)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.resolved || plan.analyzed) {
      return plan
    }

    plan transformUp {
      // Write path
      case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
        // Inserting into partitioned table is not supported in Parquet data source (yet).
        if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreParquet(r) =>
        InsertIntoTable(convertToParquetRelation(r), partition, child, overwrite, ifNotExists)

      // Write path
      case hive.InsertIntoHiveTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
        // Inserting into partitioned table is not supported in Parquet data source (yet).
        if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreParquet(r) =>
        InsertIntoTable(convertToParquetRelation(r), partition, child, overwrite, ifNotExists)

      // Read path
      case relation: MetastoreRelation if shouldConvertMetastoreParquet(relation) =>
        val parquetRelation = convertToParquetRelation(relation)
        SubqueryAlias(relation.alias.getOrElse(relation.tableName), parquetRelation)

      // Write path
      case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
        // Inserting into partitioned table is not supported in Orc data source (yet).
        if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreOrc(r) =>
        InsertIntoTable(convertToOrcRelation(r), partition, child, overwrite, ifNotExists)

      // Write path
      case hive.InsertIntoHiveTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
        // Inserting into partitioned table is not supported in Orc data source (yet).
        if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreOrc(r) =>
        InsertIntoTable(convertToOrcRelation(r), partition, child, overwrite, ifNotExists)

      // Read path
      case relation: MetastoreRelation if shouldConvertMetastoreOrc(relation) =>
        val orcRelation = convertToOrcRelation(relation)
        SubqueryAlias(relation.alias.getOrElse(relation.tableName), orcRelation)
    }
  }

  private def shouldConvertMetastoreParquet(relation: MetastoreRelation): Boolean = {
    relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") &&
      sparkSession.conf.get(HiveUtils.CONVERT_METASTORE_PARQUET.key) == "true"
  }

  private def convertToParquetRelation(relation: MetastoreRelation): LogicalRelation = {
    val defaultSource = new ParquetFileFormat()
    val fileFormatClass = classOf[ParquetFileFormat]

    val options = Map(
      ParquetFileFormat.MERGE_SCHEMA ->
        sparkSession.conf.get(HiveUtils.CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING.key),
      ParquetFileFormat.METASTORE_TABLE_NAME -> TableIdentifier(
        relation.tableName,
        Some(relation.databaseName)
      ).unquotedString
    )

    convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "parquet")
  }

  private def shouldConvertMetastoreOrc(relation: MetastoreRelation): Boolean = {
    relation.tableDesc.getSerdeClassName.toLowerCase.contains("orc") &&
    sparkSession.conf.get(HiveUtils.CONVERT_METASTORE_ORC.key) == "true"
  }

  private def convertToOrcRelation(relation: MetastoreRelation): LogicalRelation = {
    val defaultSource = new OrcFileFormat()
    val fileFormatClass = classOf[OrcFileFormat]
    val options = Map[String, String]()

    convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "orc")
  }

  private def convertToLogicalRelation(
      metastoreRelation: MetastoreRelation,
      options: Map[String, String],
      defaultSource: FileFormat,
      fileFormatClass: Class[_ <: FileFormat],
      fileType: String): LogicalRelation = {
    val metastoreSchema = StructType.fromAttributes(metastoreRelation.output)
    val tableIdentifier =
      TableIdentifier(metastoreRelation.tableName, Some(metastoreRelation.databaseName))
    val bucketSpec = None  // We don't support hive bucketed tables, only ones we write out.

    val result = if (metastoreRelation.hiveQlTable.isPartitioned) {
      val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)
      val partitionColumnDataTypes = partitionSchema.map(_.dataType)
      // We're converting the entire table into HadoopFsRelation, so predicates to Hive metastore
      // are empty.
      val partitions = metastoreRelation.getHiveQlPartitions().map { p =>
        val location = p.getLocation
        val values = InternalRow.fromSeq(p.getValues.asScala.zip(partitionColumnDataTypes).map {
          case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
        })
        PartitionDirectory(values, location)
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)

      val cached = getCached(
        tableIdentifier,
        metastoreRelation,
        metastoreSchema,
        fileFormatClass,
        bucketSpec,
        Some(partitionSpec))

      val hadoopFsRelation = cached.getOrElse {
        val fileCatalog = new MetaStorePartitionedTableFileCatalog(
          sparkSession,
          new Path(metastoreRelation.catalogTable.storage.locationUri.get),
          partitionSpec)

        val inferredSchema = if (fileType.equals("parquet")) {
          val inferredSchema =
            defaultSource.inferSchema(sparkSession, options, fileCatalog.allFiles())
          inferredSchema.map { inferred =>
            ParquetFileFormat.mergeMetastoreParquetSchema(metastoreSchema, inferred)
          }.getOrElse(metastoreSchema)
        } else {
          defaultSource.inferSchema(sparkSession, options, fileCatalog.allFiles()).get
        }

        val relation = HadoopFsRelation(
          sparkSession = sparkSession,
          location = fileCatalog,
          partitionSchema = partitionSchema,
          dataSchema = inferredSchema,
          bucketSpec = bucketSpec,
          fileFormat = defaultSource,
          options = options)

        val created = LogicalRelation(
          relation,
          metastoreTableIdentifier = Option(tableIdentifier))
        sparkSession.sessionState.catalog.putCached(tableIdentifier, created)
        created
      }

      hadoopFsRelation
    } else {
      val paths = Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)

      val cached = getCached(tableIdentifier,
        metastoreRelation,
        metastoreSchema,
        fileFormatClass,
        bucketSpec,
        None)
      val logicalRelation = cached.getOrElse {
        val created =
          LogicalRelation(
            DataSource(
              sparkSession = sparkSession,
              paths = paths,
              userSpecifiedSchema = Some(metastoreRelation.schema),
              bucketSpec = bucketSpec,
              options = options,
              className = fileType).resolveRelation(),
            metastoreTableIdentifier = Option(tableIdentifier))

        sparkSession.sessionState.catalog.putCached(tableIdentifier, created)
        created
      }

      logicalRelation
    }
    result.copy(expectedOutputAttributes = Some(metastoreRelation.output))
  }

  def getCached(
      tableIdentifier: TableIdentifier,
      metastoreRelation: MetastoreRelation,
      schemaInMetastore: StructType,
      expectedFileFormat: Class[_ <: FileFormat],
      expectedBucketSpec: Option[BucketSpec],
      partitionSpecInMetastore: Option[PartitionSpec]): Option[LogicalRelation] = {

    sparkSession.sessionState.catalog.getCached(tableIdentifier) match {
      case null => None // Cache miss
      case Some(logical @ LogicalRelation(relation: HadoopFsRelation, _, _)) =>
        val pathsInMetastore = metastoreRelation.catalogTable.storage.locationUri.toSeq
        val cachedRelationFileFormatClass = relation.fileFormat.getClass

        expectedFileFormat match {
          case `cachedRelationFileFormatClass` =>
            // If we have the same paths, same schema, and same partition spec,
            // we will use the cached relation.
            val useCached =
              relation.location.paths.map(_.toString).toSet == pathsInMetastore.toSet &&
                logical.schema.sameType(schemaInMetastore) &&
                relation.bucketSpec == expectedBucketSpec &&
                relation.partitionSpec == partitionSpecInMetastore.getOrElse {
                  PartitionSpec(StructType(Nil), Array.empty[PartitionDirectory])
                }

            if (useCached) {
              Some(logical)
            } else {
              // If the cached relation is not updated, we invalidate it right away.
              sparkSession.sessionState.catalog.invalidateTable(tableIdentifier)
              None
            }
          case _ =>
            logWarning(
              s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} " +
                s"should be stored as $expectedFileFormat. However, we are getting " +
                s"a ${relation.fileFormat} from the metastore cache. This cached " +
                s"entry will be invalidated.")
            sparkSession.sessionState.catalog.invalidateTable(tableIdentifier)
            None
        }
      case other =>
        logWarning(
          s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} should be stored " +
            s"as $expectedFileFormat. However, we are getting a $other from the metastore cache. " +
            s"This cached entry will be invalidated.")
        sparkSession.sessionState.catalog.invalidateTable(tableIdentifier)
        None
    }
  }

}

/**
 * A logical plan representing insertion into Hive table.
 * This plan ignores nullability of ArrayType, MapType, StructType unlike InsertIntoTable
 * because Hive table doesn't have nullability for ARRAY, MAP, STRUCT types.
 */
private[hive] case class InsertIntoHiveTable(
    table: MetastoreRelation,
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: Boolean,
    ifNotExists: Boolean)
  extends LogicalPlan {

  override def children: Seq[LogicalPlan] = child :: Nil
  override def output: Seq[Attribute] = Seq.empty

  val numDynamicPartitions = partition.values.count(_.isEmpty)

  // This is the expected schema of the table prepared to be inserted into,
  // including dynamic partition columns.
  val tableOutput = table.attributes ++ table.partitionKeys.takeRight(numDynamicPartitions)

  override lazy val resolved: Boolean = childrenResolved && child.output.zip(tableOutput).forall {
    case (childAttr, tableAttr) => childAttr.dataType.sameType(tableAttr.dataType)
  }
}
