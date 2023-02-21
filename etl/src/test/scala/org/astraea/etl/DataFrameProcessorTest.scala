/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.etl

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.astraea.etl.DataType.{IntegerType, StringType}
import org.astraea.etl.FileCreator.{createCSV, generateCSVF, getCSVFile}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.io._
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

class DataFrameProcessorTest {
  @Test
  def skipBlankLineTest(): Unit = {
    val sourceDir = Files.createTempDirectory("source")
    val dataDir = Files.createTempDirectory("data")
    val checkoutDir = Files.createTempDirectory("checkpoint")

    val columnOne: List[String] =
      List("A1", "B1", null, "D1")
    val columnTwo: List[String] =
      List("52", "36", null, "25")
    val columnThree: List[String] =
      List("fghgh", "gjgbn", null, "dfjf")

    val row = columnOne
      .zip(columnTwo.zip(columnThree))
      .foldLeft(List.empty[List[String]]) { case (acc, (a, (b, c))) =>
        List(a, b, c) +: acc
      }
      .reverse

    createCSV(sourceDir, row, 0)

    val df = DataFrameProcessor.fromLocalCsv(
      createSpark(),
      Metadata(
        sourcePath = sourceDir.toAbsolutePath.toString,
        checkpoint = "",
        columns = Seq(
          DataColumn("RecordNumber", true, StringType),
          DataColumn("Size", true, StringType),
          DataColumn("Type", true, StringType)
        ),
        kafkaBootstrapServers = "",
        topicName = "",
        topicConfigs = Map.empty,
        numberOfPartitions = 10,
        numberOfReplicas = 1,
        cleanSource = "delete",
        recursiveFile = "true",
        archivePath = ""
      )
    )

    df.dataFrame()
      .writeStream
      .format("csv")
      .option("path", dataDir.toAbsolutePath.toString)
      .option("checkpointLocation", checkoutDir.toAbsolutePath.toString)
      .outputMode("append")
      .start()
      .awaitTermination(Duration(20, TimeUnit.SECONDS).toMillis)

    val writeFile = getCSVFile(dataDir).head
    val lines = Files.readAllLines(writeFile)

    assertEquals("A1,52,fghgh", lines.get(0))
    assertEquals("B1,36,gjgbn", lines.get(1))
    assertEquals("D1,25,dfjf", lines.get(2))
  }

  @Test
  def sparkReadCSVTest(): Unit = {
    val sourceDir = Files.createTempDirectory("source")
    generateCSVF(sourceDir, rows)

    val checkoutDir = Files.createTempDirectory("checkpoint")
    val dataDir = Files.createTempDirectory("data")

    val csvDF = DataFrameProcessor.fromLocalCsv(
      createSpark(),
      Metadata(
        sourcePath = sourceDir.toAbsolutePath.toString,
        checkpoint = "",
        columns = Seq(
          DataColumn("RecordNumber", true, StringType),
          DataColumn("Size", true, StringType),
          DataColumn("Type", true, StringType)
        ),
        kafkaBootstrapServers = "",
        topicName = "",
        topicConfigs = Map.empty,
        numberOfPartitions = 10,
        numberOfReplicas = 1,
        cleanSource = "delete",
        recursiveFile = "true",
        archivePath = ""
      )
    )

    assertTrue(
      csvDF.dataFrame().isStreaming,
      "sessions must be a streaming Dataset"
    )

    csvDF
      .dataFrame()
      .writeStream
      .format("csv")
      .option("path", dataDir.toAbsolutePath.toString)
      .option("checkpointLocation", checkoutDir.toAbsolutePath.toString)
      .outputMode("append")
      .start()
      .awaitTermination(Duration(20, TimeUnit.SECONDS).toMillis)

    val writeFile = getCSVFile(dataDir).head
    val lines = Files.readAllLines(writeFile)
    assertEquals("A1,52,fghgh", lines.get(0))
    assertEquals("B1,36,gjgbn", lines.get(1))
    assertEquals("C1,45,fgbhjf", lines.get(2))
    assertEquals("D1,25,dfjf", lines.get(3))
  }

  @Test
  def csvToJSONTest(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Astraea ETL")
      .getOrCreate()
    import spark.implicits._

    val columns = Seq(
      DataColumn("name", isPk = true, dataType = StringType),
      DataColumn("age", isPk = false, dataType = IntegerType)
    )

    val result = new DataFrameProcessor(
      Seq(("Michael", 29)).toDF().toDF("name", "age")
    ).csvToJSON(columns)
      .dataFrame()
      .collectAsList()
      .asScala
      .map(row => (row.getAs[String]("key"), row.getAs[String]("value")))
      .toMap

    assertEquals(1, result.size)
    assertEquals(
      "{\"age\":\"29\",\"name\":\"Michael\"}",
      result("{\"name\":\"Michael\"}")
    )

    val resultExchange = new DataFrameProcessor(
      Seq((29, "Michael")).toDF().toDF("age", "name")
    ).csvToJSON(columns)
      .dataFrame()
      .collectAsList()
      .asScala
      .map(row => (row.getAs[String]("key"), row.getAs[String]("value")))
      .toMap

    assertEquals(1, resultExchange.size)
    assertEquals(
      "{\"age\":\"29\",\"name\":\"Michael\"}",
      resultExchange("{\"name\":\"Michael\"}")
    )
  }

  @Test
  def csvToJsonMulKeysTest(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Astraea ETL")
      .getOrCreate()
    import spark.implicits._
    val columns = Seq(
      DataColumn("firstName", isPk = true, DataType.StringType),
      DataColumn("secondName", isPk = true, DataType.StringType),
      DataColumn("age", isPk = false, dataType = IntegerType)
    )
    val result = new DataFrameProcessor(
      Seq(("Michael", "A", 29)).toDF().toDF("firstName", "secondName", "age")
    ).csvToJSON(columns)
      .dataFrame()
      .collectAsList()
      .asScala
      .map(row => (row.getAs[String]("key"), row.getAs[String]("value")))
      .toMap

    assertEquals(1, result.size)
    assertEquals(
      "{\"age\":\"29\",\"firstName\":\"Michael\",\"secondName\":\"A\"}",
      result("{\"firstName\":\"Michael\",\"secondName\":\"A\"}")
    )
  }

  @Test def csvToJsonNullTest(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Astraea ETL")
      .getOrCreate()
    import spark.implicits._
    val columns = Seq(
      DataColumn("firstName", isPk = true, DataType.StringType),
      DataColumn("secondName", isPk = true, DataType.StringType),
      DataColumn("age", isPk = false, dataType = IntegerType)
    )
    val result = new DataFrameProcessor(
      Seq(("Michael", "A", null)).toDF().toDF("firstName", "secondName", "age")
    ).csvToJSON(columns)
      .dataFrame()
      .collectAsList()
      .asScala
      .map(row => (row.getAs[String]("key"), row.getAs[String]("value")))
      .toMap

    assertEquals(1, result.size)
    assertEquals(
      "{\"firstName\":\"Michael\",\"secondName\":\"A\"}",
      result("{\"firstName\":\"Michael\",\"secondName\":\"A\"}")
    )
  }

  @Test
  def jsonToByteTest(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Astraea ETL")
      .getOrCreate()

    var data = Seq(Row("A1", 52, "fghgh", "sfjojs", "zzz", "final", 5))
    (0 to 10000).iterator.foreach(_ =>
      data = data ++ Seq(Row("A1", 52, "fghgh", "sfjojs", "zzz", "final", 5))
    )

    val structType = new StructType()
      .add("name", "string")
      .add("age", "integer")
      .add("xx", "string")
      .add("yy", "string")
      .add("zz", "string")
      .add("f", "string")
      .add("fInt", "integer")

    val columns = Seq(
      DataColumn("name", isPk = false, dataType = StringType),
      DataColumn("age", isPk = false, dataType = IntegerType),
      DataColumn("xx", isPk = false, dataType = StringType),
      DataColumn("yy", isPk = false, dataType = StringType),
      DataColumn("zz", isPk = false, dataType = StringType),
      DataColumn("f", isPk = false, dataType = StringType),
      DataColumn("fInt", isPk = false, dataType = IntegerType)
    )

    val json = new DataFrameProcessor(
      spark.createDataFrame(spark.sparkContext.parallelize(data), structType)
    ).csvToJSON(columns)
      .dataFrame()
      .withColumn("byte", col("value").cast("Byte"))
      .selectExpr("CAST(byte AS BYTE)")
    val head = json.head()
    assertTrue(json.filter(_ != head).isEmpty)
  }

  private[this] def rows: List[List[String]] = {
    val columnOne: List[String] =
      List("A1", "B1", "C1", "D1")
    val columnTwo: List[String] =
      List("52", "36", "45", "25")
    val columnThree: List[String] =
      List("fghgh", "gjgbn", "fgbhjf", "dfjf")

    columnOne
      .zip(columnTwo.zip(columnThree))
      .foldLeft(List.empty[List[String]]) { case (acc, (a, (b, c))) =>
        List(a, b, c) +: acc
      }
      .reverse
  }

  private def createSpark(): SparkSession = {
    SparkSession
      .builder()
      .master("local[2]")
      .appName("Astraea ETL")
      .getOrCreate()
  }
}
