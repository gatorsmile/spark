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

import java.io.File

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.orc.OrcConf.COMPRESS
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.execution.datasources.orc.OrcOptions
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetTest}
import org.apache.spark.sql.hive.orc.OrcFileOperator
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf

class CompressionCodecSuite extends TestHiveSingleton with ParquetTest with BeforeAndAfterAll {
  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    (0 until maxRecordNum).toDF("a").createOrReplaceTempView("table_source")
  }

  override def afterAll(): Unit = {
    try {
      spark.catalog.dropTempView("table_source")
    } finally {
      super.afterAll()
    }
  }

  private val maxRecordNum = 100000

  private def getConvertMetastoreConfName(format: String): String = format.toLowerCase match {
    case "parquet" => HiveUtils.CONVERT_METASTORE_PARQUET.key
    case "orc" => HiveUtils.CONVERT_METASTORE_ORC.key
  }

  private def getSparkCompressionConfName(format: String): String = format.toLowerCase match {
    case "parquet" => SQLConf.PARQUET_COMPRESSION.key
    case "orc" => SQLConf.ORC_COMPRESSION.key
  }

  private def getHiveCompressPropName(format: String): String = format.toLowerCase match {
    case "parquet" => ParquetOutputFormat.COMPRESSION
    case "orc" => COMPRESS.getAttribute
  }

  private def normalizeCodecName(format: String, name: String): String = {
    format.toLowerCase match {
      case "parquet" => ParquetOptions.shortParquetCompressionCodecNames(name).name()
      case "orc" => OrcOptions.shortOrcCompressionCodecNames(name)
    }
  }

  private def getTableCompressionCodec(path: String, format: String): String = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val codecs = format.toLowerCase match {
      case "parquet" => for {
        footer <- readAllFootersWithoutSummaryFiles(new Path(path), hadoopConf)
        block <- footer.getParquetMetadata.getBlocks.asScala
        column <- block.getColumns.asScala
      } yield column.getCodec.name()
      case "orc" => new File(path).listFiles().filter{ file =>
        file.isFile && !file.getName.endsWith(".crc") && file.getName != "_SUCCESS"
      }.map { orcFile =>
        OrcFileOperator.getFileReader(orcFile.toPath.toString).get.getCompression.toString
      }.toSeq
    }

    assert(codecs.distinct.length == 1)
    codecs.head
  }

  private def writeDataToTable(
      rootDir: File,
      tableName: String,
      isPartitioned: Boolean,
      format: String,
      compressionCodec: Option[String]): Unit = {
    val tblProperties = compressionCodec match {
      case Some(prop) => s"TBLPROPERTIES('${getHiveCompressPropName(format)}'='$prop')"
      case _ => ""
    }
    val partitionCreate = if (isPartitioned) "PARTITIONED BY (p int)" else ""
    sql(
      s"""
         |CREATE TABLE $tableName(a int)
         |$partitionCreate
         |STORED AS $format
         |LOCATION '${rootDir.toURI.toString.stripSuffix("/")}/$tableName'
         |$tblProperties
       """.stripMargin)

    val partitionInsert = if (isPartitioned) s"partition (p=10000)" else ""
    sql(
      s"""
         |INSERT OVERWRITE TABLE $tableName
         |$partitionInsert
         |SELECT * FROM table_source
       """.stripMargin)
  }

  private def getTableSize(path: String): Long = {
    val dir = new File(path)
    val files = dir.listFiles().filter(_.getName.startsWith("part-"))
    files.map(_.length()).sum
  }

  private def getUncompressedDataSizeByFormat(
      format: String, isPartitioned: Boolean): Long = {
    var totalSize = 0L
    val tableName = s"tbl_$format"
    val codecName = normalizeCodecName(format, "uncompressed")
    withSQLConf(getSparkCompressionConfName(format) -> codecName) {
      withTempDir { tmpDir =>
        withTable(tableName) {
          writeDataToTable(tmpDir, tableName, isPartitioned, format, Option(codecName))
          val partition = if (isPartitioned) "p=10000" else ""
          val path = s"${tmpDir.getPath.stripSuffix("/")}/$tableName/$partition"
          totalSize = getTableSize(path)
        }
      }
    }
    assert(totalSize > 0L)
    totalSize
  }

  private def checkCompressionCodecForTable(
      format: String,
      isPartitioned: Boolean,
      compressionCodec: Option[String])
      (assertion: (String, Long) => Unit): Unit = {
    val tableName = s"tbl_$format$isPartitioned"
    withTempDir { tmpDir =>
      withTable(tableName) {
        writeDataToTable(tmpDir, tableName, isPartitioned, format, compressionCodec)
        val partition = if (isPartitioned) "p=10000" else ""
        val path = s"${tmpDir.getPath.stripSuffix("/")}/$tableName/$partition"
        val relCompressionCodec = getTableCompressionCodec(path, format)
        val tableSize = getTableSize(path)
        assertion(relCompressionCodec, tableSize)
      }
    }
  }

  private def checkTableCompressionCodecForCodecs(
      format: String,
      isPartitioned: Boolean,
      convertMetastore: Boolean,
      compressionCodecs: List[String],
      tableCompressionCodecs: List[String])
      (assertionCompressionCodec: (Option[String], String, String, Long) => Unit): Unit = {
    withSQLConf(getConvertMetastoreConfName(format) -> convertMetastore.toString) {
      tableCompressionCodecs.foreach { tableCompression =>
        compressionCodecs.foreach { sessionCompressionCodec =>
          withSQLConf(getSparkCompressionConfName(format) -> sessionCompressionCodec) {
            // 'tableCompression = null' means no table-level compression
            val compression = Option(tableCompression)
            checkCompressionCodecForTable(format, isPartitioned, compression) {
              case (realCompressionCodec, tableSize) => assertionCompressionCodec(compression,
                sessionCompressionCodec, realCompressionCodec, tableSize)
            }
          }
        }
      }
    }
  }

  // When the amount of data is small, compressed data size may be larger than uncompressed one,
  // so we just check the difference when compressionCodec is not NONE or UNCOMPRESSED.
  // When convertMetastore is false, the uncompressed table size should be same as
  // `partitionedParquetTableUncompressedSize`, regardless of whether it is partitioned.
  private def checkTableSize(
      format: String,
      compressionCodec: String,
      isPartitioned: Boolean,
      convertMetastore: Boolean,
      tableSize: Long): Boolean = {
    format match {
      case "parquet" =>
        val uncompressedSize = if (!convertMetastore || isPartitioned) {
          getUncompressedDataSizeByFormat(format, isPartitioned = true)
        } else {
          getUncompressedDataSizeByFormat(format, isPartitioned = false)
        }

        if (compressionCodec == "UNCOMPRESSED") {
          tableSize == uncompressedSize
        } else {
          tableSize != uncompressedSize
        }
      case "orc" =>
        val uncompressedSize = if (!convertMetastore || isPartitioned) {
          getUncompressedDataSizeByFormat(format, isPartitioned = true)
        } else {
          getUncompressedDataSizeByFormat(format, isPartitioned = false)
        }
        if (compressionCodec == "NONE") {
          tableSize == uncompressedSize
        } else {
          tableSize != uncompressedSize
        }
      case _ => false
    }
  }

  def checkForTableWithCompressProp(format: String, compressCodecs: List[String]): Unit = {
    Seq(true, false).foreach { isPartitioned =>
      Seq(true, false).foreach { convertMetastore =>
        checkTableCompressionCodecForCodecs(
          format,
          isPartitioned,
          convertMetastore,
          compressionCodecs = compressCodecs,
          tableCompressionCodecs = compressCodecs) {
          case (tableCompressionCodec, sessionCompressionCodec, realCompressionCodec, tableSize) =>
            xyz
        }
      }
    }
  }

  def checkForTableWithoutCompressProp(format: String, compressCodecs: List[String]): Unit = {
    Seq(true, false).foreach { isPartitioned =>
      Seq(true, false).foreach { convertMetastore =>
        checkTableCompressionCodecForCodecs(
          format,
          isPartitioned,
          convertMetastore,
          compressionCodecs = compressCodecs,
          tableCompressionCodecs = List(null)) {
          case (tableCompressionCodec, sessionCompressionCodec, realCompressionCodec, tableSize) =>
            xyz
        }
      }
    }
  }

  test("both table-level and session-level compression are set") {
    checkForTableWithCompressProp("parquet", List("UNCOMPRESSED", "SNAPPY", "GZIP"))
    checkForTableWithCompressProp("orc", List("NONE", "SNAPPY", "ZLIB"))
  }

  test("table-level compression is not set but session-level compressions is set ") {
    checkForTableWithoutCompressProp("parquet", List("UNCOMPRESSED", "SNAPPY", "GZIP"))
    checkForTableWithoutCompressProp("orc", List("NONE", "SNAPPY", "ZLIB"))
  }

}
