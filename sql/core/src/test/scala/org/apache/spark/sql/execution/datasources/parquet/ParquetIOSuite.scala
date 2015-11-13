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

package org.apache.spark.sql.execution.datasources.parquet

import java.util.Collections

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.example.data.{Group, GroupWriter}
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.hadoop.metadata.{CompressionCodecName, FileMetaData, ParquetMetadata}
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.{MessageType, MessageTypeParser}

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

// Write support class for nested groups: ParquetWriter initializes GroupWriteSupport
// with an empty configuration (it is after all not intended to be used in this way?)
// and members are private so we need to make our own in order to pass the schema
// to the writer.
private[parquet] class TestGroupWriteSupport(schema: MessageType) extends WriteSupport[Group] {
  var groupWriter: GroupWriter = null

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    groupWriter = new GroupWriter(recordConsumer, schema)
  }

  override def init(configuration: Configuration): WriteContext = {
    new WriteContext(schema, new java.util.HashMap[String, String]())
  }

  override def write(record: Group) {
    groupWriter.write(record)
  }
}

/**
 * A test suite that tests basic Parquet I/O.
 */
class ParquetIOSuite extends QueryTest with ParquetTest with SharedSQLContext {
  import testImplicits._

  /**
   * Writes `data` to a Parquet file, reads it back and check file contents.
   */
  protected def checkParquetFile[T <: Product : ClassTag: TypeTag](data: Seq[T]): Unit = {
    withParquetDataFrame(data)(r => checkAnswer(r, data.map(Row.fromTuple)))
  }

  test("basic data types (without binary)") {
    val data = (1 to 4).map { i =>
      (i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble)
    }
    checkParquetFile(data)
  }

  test("raw binary") {
    val data = (1 to 4).map(i => Tuple1(Array.fill(3)(i.toByte)))
    withParquetDataFrame(data) { df =>
      assertResult(data.map(_._1.mkString(",")).sorted) {
        df.collect().map(_.getAs[Array[Byte]](0).mkString(",")).sorted
      }
    }
  }

  test("string") {
    val data = (1 to 4).map(i => Tuple1(i.toString))
    // Property spark.sql.parquet.binaryAsString shouldn't affect Parquet files written by Spark SQL
    // as we store Spark SQL schema in the extra metadata.
    withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "false")(checkParquetFile(data))
    withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "true")(checkParquetFile(data))
  }

  testStandardAndLegacyModes("fixed-length decimals") {
    def makeDecimalRDD(decimal: DecimalType): DataFrame = {
      sqlContext
        .range(1000)
        // Parquet doesn't allow column names with spaces, have to add an alias here.
        // Minus 500 here so that negative decimals are also tested.
        .select((('id - 500) / 100.0) cast decimal as 'dec)
        .coalesce(1)
    }

    val combinations = Seq((5, 2), (1, 0), (1, 1), (18, 10), (18, 17), (19, 0), (38, 37))
    for ((precision, scale) <- combinations) {
      withTempPath { dir =>
        val data = makeDecimalRDD(DecimalType(precision, scale))
        data.write.parquet(dir.getCanonicalPath)
        checkAnswer(sqlContext.read.parquet(dir.getCanonicalPath), data.collect().toSeq)
      }
    }
  }

  test("date type") {
    def makeDateRDD(): DataFrame =
      sparkContext
        .parallelize(0 to 1000)
        .map(i => Tuple1(DateTimeUtils.toJavaDate(i)))
        .toDF()
        .select($"_1")

    withTempPath { dir =>
      val data = makeDateRDD()
      data.write.parquet(dir.getCanonicalPath)
      checkAnswer(sqlContext.read.parquet(dir.getCanonicalPath), data.collect().toSeq)
    }
  }

  testStandardAndLegacyModes("map") {
    val data = (1 to 4).map(i => Tuple1(Map(i -> s"val_$i")))
    checkParquetFile(data)
  }

  testStandardAndLegacyModes("array") {
    val data = (1 to 4).map(i => Tuple1(Seq(i, i + 1)))
    checkParquetFile(data)
  }

  testStandardAndLegacyModes("array and double") {
    val data = (1 to 4).map(i => (i.toDouble, Seq(i.toDouble, (i + 1).toDouble)))
    checkParquetFile(data)
  }

  testStandardAndLegacyModes("struct") {
    val data = (1 to 4).map(i => Tuple1((i, s"val_$i")))
    withParquetDataFrame(data) { df =>
      // Structs are converted to `Row`s
      checkAnswer(df, data.map { case Tuple1(struct) =>
        Row(Row(struct.productIterator.toSeq: _*))
      })
    }
  }

  testStandardAndLegacyModes("nested struct with array of array as field") {
    val data = (1 to 4).map(i => Tuple1((i, Seq(Seq(s"val_$i")))))
    withParquetDataFrame(data) { df =>
      // Structs are converted to `Row`s
      checkAnswer(df, data.map { case Tuple1(struct) =>
        Row(Row(struct.productIterator.toSeq: _*))
      })
    }
  }

  testStandardAndLegacyModes("nested map with struct as value type") {
    val data = (1 to 4).map(i => Tuple1(Map(i -> (i, s"val_$i"))))
    withParquetDataFrame(data) { df =>
      checkAnswer(df, data.map { case Tuple1(m) =>
        Row(m.mapValues(struct => Row(struct.productIterator.toSeq: _*)))
      })
    }
  }

  test("nulls") {
    val allNulls = (
      null.asInstanceOf[java.lang.Boolean],
      null.asInstanceOf[Integer],
      null.asInstanceOf[java.lang.Long],
      null.asInstanceOf[java.lang.Float],
      null.asInstanceOf[java.lang.Double])

    withParquetDataFrame(allNulls :: Nil) { df =>
      val rows = df.collect()
      assert(rows.length === 1)
      assert(rows.head === Row(Seq.fill(5)(null): _*))
    }
  }

  test("nones") {
    val allNones = (
      None.asInstanceOf[Option[Int]],
      None.asInstanceOf[Option[Long]],
      None.asInstanceOf[Option[String]])

    withParquetDataFrame(allNones :: Nil) { df =>
      val rows = df.collect()
      assert(rows.length === 1)
      assert(rows.head === Row(Seq.fill(3)(null): _*))
    }
  }

  test("SPARK-10113 Support for unsigned Parquet logical types") {
    val parquetSchema = MessageTypeParser.parseMessageType(
      """message root {
        |  required int32 c(UINT_32);
        |}
      """.stripMargin)

    withTempPath { location =>
      val extraMetadata = Map.empty[String, String].asJava
      val fileMetadata = new FileMetaData(parquetSchema, extraMetadata, "Spark")
      val path = new Path(location.getCanonicalPath)
      val footer = List(
        new Footer(path, new ParquetMetadata(fileMetadata, Collections.emptyList()))
      ).asJava

      ParquetFileWriter.writeMetadataFile(sparkContext.hadoopConfiguration, path, footer)

      val errorMessage = intercept[Throwable] {
        sqlContext.read.parquet(path.toString).printSchema()
      }.toString
      assert(errorMessage.contains("Parquet type not supported"))
    }
  }

  test("compression codec") {
    def compressionCodecFor(path: String, codecName: String): String = {
      val codecs = for {
        footer <- readAllFootersWithoutSummaryFiles(new Path(path), hadoopConfiguration)
        block <- footer.getParquetMetadata.getBlocks.asScala
        column <- block.getColumns.asScala
      } yield column.getCodec.name()

      assert(codecs.distinct === Seq(codecName))
      codecs.head
    }

    val data = (0 until 10).map(i => (i, i.toString))

    def checkCompressionCodec(codec: CompressionCodecName): Unit = {
      withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> codec.name()) {
        withParquetFile(data) { path =>
          assertResult(sqlContext.conf.parquetCompressionCodec.toUpperCase) {
            compressionCodecFor(path, codec.name())
          }
        }
      }
    }

    // Checks default compression codec
    checkCompressionCodec(CompressionCodecName.fromConf(sqlContext.conf.parquetCompressionCodec))

    checkCompressionCodec(CompressionCodecName.UNCOMPRESSED)
    checkCompressionCodec(CompressionCodecName.GZIP)
    checkCompressionCodec(CompressionCodecName.SNAPPY)
  }

  test("read raw Parquet file") {
    def makeRawParquetFile(path: Path): Unit = {
      val schema = MessageTypeParser.parseMessageType(
        """
          |message root {
          |  required boolean _1;
          |  required int32   _2;
          |  required int64   _3;
          |  required float   _4;
          |  required double  _5;
          |}
        """.stripMargin)

      val writeSupport = new TestGroupWriteSupport(schema)
      val writer = new ParquetWriter[Group](path, writeSupport)

      (0 until 10).foreach { i =>
        val record = new SimpleGroup(schema)
        record.add(0, i % 2 == 0)
        record.add(1, i)
        record.add(2, i.toLong)
        record.add(3, i.toFloat)
        record.add(4, i.toDouble)
        writer.write(record)
      }

      writer.close()
    }

    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "part-r-0.parquet")
      makeRawParquetFile(path)
      checkAnswer(sqlContext.read.parquet(path.toString), (0 until 10).map { i =>
        Row(i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble)
      })
    }
  }

  test("write metadata") {
    withTempPath { file =>
      val path = new Path(file.toURI.toString)
      val fs = FileSystem.getLocal(hadoopConfiguration)
      val schema = StructType.fromAttributes(ScalaReflection.attributesFor[(Int, String)])
      writeMetadata(schema, path, hadoopConfiguration)

      assert(fs.exists(new Path(path, ParquetFileWriter.PARQUET_COMMON_METADATA_FILE)))
      assert(fs.exists(new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)))

      val expectedSchema = new CatalystSchemaConverter().convert(schema)
      val actualSchema = readFooter(path, hadoopConfiguration).getFileMetaData.getSchema

      actualSchema.checkContains(expectedSchema)
      expectedSchema.checkContains(actualSchema)
    }
  }

  test("save - overwrite") {
    withParquetFile((1 to 10).map(i => (i, i.toString))) { file =>
      val newData = (11 to 20).map(i => (i, i.toString))
      newData.toDF().write.format("parquet").mode(SaveMode.Overwrite).save(file)
      checkAnswer(sqlContext.read.parquet(file), newData.map(Row.fromTuple))
    }
  }

  test("save - ignore") {
    val data = (1 to 10).map(i => (i, i.toString))
    withParquetFile(data) { file =>
      val newData = (11 to 20).map(i => (i, i.toString))
      newData.toDF().write.format("parquet").mode(SaveMode.Ignore).save(file)
      checkAnswer(sqlContext.read.parquet(file), data.map(Row.fromTuple))
    }
  }

  test("save - throw") {
    val data = (1 to 10).map(i => (i, i.toString))
    withParquetFile(data) { file =>
      val newData = (11 to 20).map(i => (i, i.toString))
      val errorMessage = intercept[Throwable] {
        newData.toDF().write.format("parquet").mode(SaveMode.ErrorIfExists).save(file)
      }.getMessage
      assert(errorMessage.contains("already exists"))
    }
  }

  test("save - append") {
    val data = (1 to 10).map(i => (i, i.toString))
    withParquetFile(data) { file =>
      val newData = (11 to 20).map(i => (i, i.toString))
      newData.toDF().write.format("parquet").mode(SaveMode.Append).save(file)
      checkAnswer(sqlContext.read.parquet(file), (data ++ newData).map(Row.fromTuple))
    }
  }

  test("SPARK-6315 regression test") {
    // Spark 1.1 and prior versions write Spark schema as case class string into Parquet metadata.
    // This has been deprecated by JSON format since 1.2.  Notice that, 1.3 further refactored data
    // types API, and made StructType.fields an array.  This makes the result of StructType.toString
    // different from prior versions: there's no "Seq" wrapping the fields part in the string now.
    val sparkSchema =
      "StructType(Seq(StructField(a,BooleanType,false),StructField(b,IntegerType,false)))"

    // The Parquet schema is intentionally made different from the Spark schema.  Because the new
    // Parquet data source simply falls back to the Parquet schema once it fails to parse the Spark
    // schema.  By making these two different, we are able to assert the old style case class string
    // is parsed successfully.
    val parquetSchema = MessageTypeParser.parseMessageType(
      """message root {
        |  required int32 c;
        |}
      """.stripMargin)

    withTempPath { location =>
      val extraMetadata = Collections.singletonMap(
        CatalystReadSupport.SPARK_METADATA_KEY, sparkSchema.toString)
      val fileMetadata = new FileMetaData(parquetSchema, extraMetadata, "Spark")
      val path = new Path(location.getCanonicalPath)

      ParquetFileWriter.writeMetadataFile(
        sparkContext.hadoopConfiguration,
        path,
        Collections.singletonList(
          new Footer(path, new ParquetMetadata(fileMetadata, Collections.emptyList()))))

      assertResult(sqlContext.read.parquet(path.toString).schema) {
        StructType(
          StructField("a", BooleanType, nullable = false) ::
          StructField("b", IntegerType, nullable = false) ::
          Nil)
      }
    }
  }

  test("SPARK-6352 DirectParquetOutputCommitter") {
    val clonedConf = new Configuration(hadoopConfiguration)

    // Write to a parquet file and let it fail.
    // _temporary should be missing if direct output committer works.
    try {
      hadoopConfiguration.set("spark.sql.parquet.output.committer.class",
        classOf[DirectParquetOutputCommitter].getCanonicalName)
      sqlContext.udf.register("div0", (x: Int) => x / 0)
      withTempPath { dir =>
        intercept[org.apache.spark.SparkException] {
          sqlContext.sql("select div0(1)").write.parquet(dir.getCanonicalPath)
        }
        val path = new Path(dir.getCanonicalPath, "_temporary")
        val fs = path.getFileSystem(hadoopConfiguration)
        assert(!fs.exists(path))
      }
    } finally {
      // Hadoop 1 doesn't have `Configuration.unset`
      hadoopConfiguration.clear()
      clonedConf.asScala.foreach(entry => hadoopConfiguration.set(entry.getKey, entry.getValue))
    }
  }

  test("SPARK-9849 DirectParquetOutputCommitter qualified name should be backward compatible") {
    val clonedConf = new Configuration(hadoopConfiguration)

    // Write to a parquet file and let it fail.
    // _temporary should be missing if direct output committer works.
    try {
      hadoopConfiguration.set("spark.sql.parquet.output.committer.class",
        "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
      sqlContext.udf.register("div0", (x: Int) => x / 0)
      withTempPath { dir =>
        intercept[org.apache.spark.SparkException] {
          sqlContext.sql("select div0(1)").write.parquet(dir.getCanonicalPath)
        }
        val path = new Path(dir.getCanonicalPath, "_temporary")
        val fs = path.getFileSystem(hadoopConfiguration)
        assert(!fs.exists(path))
      }
    } finally {
      // Hadoop 1 doesn't have `Configuration.unset`
      hadoopConfiguration.clear()
      clonedConf.asScala.foreach(entry => hadoopConfiguration.set(entry.getKey, entry.getValue))
    }
  }


  test("SPARK-8121: spark.sql.parquet.output.committer.class shouldn't be overridden") {
    withTempPath { dir =>
      val clonedConf = new Configuration(hadoopConfiguration)

      hadoopConfiguration.set(
        SQLConf.OUTPUT_COMMITTER_CLASS.key, classOf[ParquetOutputCommitter].getCanonicalName)

      hadoopConfiguration.set(
        "spark.sql.parquet.output.committer.class",
        classOf[JobCommitFailureParquetOutputCommitter].getCanonicalName)

      try {
        val message = intercept[SparkException] {
          sqlContext.range(0, 1).write.parquet(dir.getCanonicalPath)
        }.getCause.getMessage
        assert(message === "Intentional exception for testing purposes")
      } finally {
        // Hadoop 1 doesn't have `Configuration.unset`
        hadoopConfiguration.clear()
        clonedConf.asScala.foreach(entry => hadoopConfiguration.set(entry.getKey, entry.getValue))
      }
    }
  }

  test("SPARK-6330 regression test") {
    // In 1.3.0, save to fs other than file: without configuring core-site.xml would get:
    // IllegalArgumentException: Wrong FS: hdfs://..., expected: file:///
    intercept[Throwable] {
      sqlContext.read.parquet("file:///nonexistent")
    }
    val errorMessage = intercept[Throwable] {
      sqlContext.read.parquet("hdfs://nonexistent")
    }.toString
    assert(errorMessage.contains("UnknownHostException"))
  }

  test("SPARK-7837 Do not close output writer twice when commitTask() fails") {
    val clonedConf = new Configuration(hadoopConfiguration)

    // Using a output committer that always fail when committing a task, so that both
    // `commitTask()` and `abortTask()` are invoked.
    hadoopConfiguration.set(
      "spark.sql.parquet.output.committer.class",
      classOf[TaskCommitFailureParquetOutputCommitter].getCanonicalName)

    try {
      // Before fixing SPARK-7837, the following code results in an NPE because both
      // `commitTask()` and `abortTask()` try to close output writers.

      withTempPath { dir =>
        val m1 = intercept[SparkException] {
          sqlContext.range(1).coalesce(1).write.parquet(dir.getCanonicalPath)
        }.getCause.getMessage
        assert(m1.contains("Intentional exception for testing purposes"))
      }

      withTempPath { dir =>
        val m2 = intercept[SparkException] {
          val df = sqlContext.range(1).select('id as 'a, 'id as 'b).coalesce(1)
          df.write.partitionBy("a").parquet(dir.getCanonicalPath)
        }.getCause.getMessage
        assert(m2.contains("Intentional exception for testing purposes"))
      }
    } finally {
      // Hadoop 1 doesn't have `Configuration.unset`
      hadoopConfiguration.clear()
      clonedConf.asScala.foreach(entry => hadoopConfiguration.set(entry.getKey, entry.getValue))
    }
  }

  test("read dictionary encoded decimals written as INT32") {
    checkAnswer(
      // Decimal column in this file is encoded using plain dictionary
      readResourceParquetFile("dec-in-i32.parquet"),
      sqlContext.range(1 << 4).select('id % 10 cast DecimalType(5, 2) as 'i32_dec))
  }

  test("read dictionary encoded decimals written as INT64") {
    checkAnswer(
      // Decimal column in this file is encoded using plain dictionary
      readResourceParquetFile("dec-in-i64.parquet"),
      sqlContext.range(1 << 4).select('id % 10 cast DecimalType(10, 2) as 'i64_dec))
  }

  // TODO Adds test case for reading dictionary encoded decimals written as `FIXED_LEN_BYTE_ARRAY`
  // The Parquet writer version Spark 1.6 and prior versions use is `PARQUET_1_0`, which doesn't
  // provide dictionary encoding support for `FIXED_LEN_BYTE_ARRAY`.  Should add a test here once
  // we upgrade to `PARQUET_2_0`.
}

class JobCommitFailureParquetOutputCommitter(outputPath: Path, context: TaskAttemptContext)
  extends ParquetOutputCommitter(outputPath, context) {

  override def commitJob(jobContext: JobContext): Unit = {
    sys.error("Intentional exception for testing purposes")
  }
}

class TaskCommitFailureParquetOutputCommitter(outputPath: Path, context: TaskAttemptContext)
  extends ParquetOutputCommitter(outputPath, context) {

  override def commitTask(context: TaskAttemptContext): Unit = {
    sys.error("Intentional exception for testing purposes")
  }
}
