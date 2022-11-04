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

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.apache.spark.sql.types.StructType
import org.astraea.etl.DataType.{IntegerType, StringType}
import org.astraea.etl.FileCreator.{createCSV, generateCSVF, getCSVFile, mkdir}
import org.astraea.etl.Reader.createSpark
import org.astraea.it.RequireBrokerCluster
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{Disabled, Test}

import java.io._
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.Random

class ReaderTest extends RequireBrokerCluster {
  @Test def pkNonNullTest(): Unit = {
    val tempPath: String =
      System.getProperty("java.io.tmpdir") + "/createSchemaNullTest" + Random
        .nextInt()
    mkdir(tempPath)

    val tempArchivePath: String =
      System.getProperty("java.io.tmpdir") + "/createSchemaNullTest" + Random
        .nextInt()
    mkdir(tempArchivePath)

    val columnOne: List[String] =
      List("A1", "B1", "C1", null)
    val columnTwo: List[String] =
      List("52", "36", "45", "25")
    val columnThree: List[String] =
      List("fghgh", "gjgbn", "fgbhjf", "dfjf")

    val row = columnOne
      .zip(columnTwo.zip(columnThree))
      .foldLeft(List.empty[List[String]]) { case (acc, (a, (b, c))) =>
        List(a, b, c) +: acc
      }
      .reverse

    createCSV(new File(tempPath), row, 0)

    val structType = Reader.createSchema(
      Map(
        "SerialNumber" -> IntegerType,
        "RecordNumber" -> StringType,
        "Size" -> IntegerType,
        "Type" -> StringType
      )
    )

    val df =
      Reader
        .of()
        .spark("local[2]")
        .schema(structType)
        .sinkPath(new File(tempArchivePath).getPath)
        .primaryKeys(Seq("RecordNumber"))
        .readCSV(new File(tempPath).getPath)
        .dataFrame()

    assertThrows(
      classOf[StreamingQueryException],
      () =>
        df.writeStream
          .outputMode(OutputMode.Append())
          .format("console")
          .start()
          .awaitTermination(Duration(5, TimeUnit.SECONDS).toMillis)
    )
  }

  @Test def sparkReadCSVTest(): Unit = {
    val tempPath =
      System.getProperty("java.io.tmpdir") + "/sparkFile-" + Random.nextInt()
    val myDir = mkdir(tempPath)
    val sourceDir = mkdir(myDir.getPath + "/source")
    generateCSVF(sourceDir, rows)
    val sinkDir = mkdir(tempPath + "/sink")
    val checkoutDir = mkdir(tempPath + "/checkout")
    val dataDir = mkdir(tempPath + "/data")

    val structType = Reader.createSchema(
      Map(
        "SerialNumber" -> IntegerType,
        "RecordNumber" -> StringType,
        "Size" -> IntegerType,
        "Type" -> StringType
      )
    )

    assertEquals(structType.length, 4)
    val csvDF = Reader
      .of()
      .spark("local[2]")
      .schema(structType)
      .sinkPath(sinkDir.getPath)
      .primaryKeys(Seq("SerialNumber"))
      .readCSV(sourceDir.getPath)
    assertTrue(
      csvDF.dataFrame().isStreaming,
      "sessions must be a streaming Dataset"
    )

    csvDF
      .dataFrame()
      .writeStream
      .format("csv")
      .option("path", dataDir.getPath)
      .option("checkpointLocation", checkoutDir.getPath)
      .outputMode("append")
      .start()
      .awaitTermination(Duration(20, TimeUnit.SECONDS).toMillis)

    val writeFile = getCSVFile(new File(dataDir.getPath)).head
    val br = new BufferedReader(new FileReader(writeFile))

    assertEquals(br.readLine, "1,A1,52,fghgh")
    assertEquals(br.readLine, "2,B1,36,gjgbn")
    assertEquals(br.readLine, "3,C1,45,fgbhjf")
    assertEquals(br.readLine, "4,D1,25,dfjf")

    Files.exists(
      new File(
        sinkDir + sourceDir.getPath + "/local_kafka-0" + ".csv"
      ).toPath
    )
  }

  @Test def csvToJSONTest(): Unit = {
    val spark = createSpark("local[2]")
    import spark.implicits._

    val columns = Seq(
      DataColumn("name", isPK = true, dataType = StringType),
      DataColumn("age", isPK = false, dataType = IntegerType)
    )

    val result = new DataFrameOp(
      Seq(("Michael", 29)).toDF().toDF("name", "age")
    ).csvToJSON(columns)
      .dataFrame()
      .collectAsList()
      .asScala
      .map(row => (row.getAs[String]("key"), row.getAs[String]("value")))
      .toMap

    assertEquals(1, result.size)
    assertEquals(
      "{\"name\":\"Michael\",\"age\":\"29\"}",
      result("{\"name\":\"Michael\"}")
    )
  }

  @Test def csvToJsonMulKeysTest(): Unit = {
    val spark = createSpark("local[2]")
    import spark.implicits._
    val columns = Seq(
      DataColumn("firstName", isPK = true, DataType.StringType),
      DataColumn("secondName", isPK = true, DataType.StringType),
      DataColumn("age", isPK = false, dataType = IntegerType)
    )
    val result = new DataFrameOp(
      Seq(("Michael", "A", 29)).toDF().toDF("firstName", "secondName", "age")
    ).csvToJSON(columns)
      .dataFrame()
      .collectAsList()
      .asScala
      .map(row => (row.getAs[String]("key"), row.getAs[String]("value")))
      .toMap

    assertEquals(1, result.size)
    assertEquals(
      "{\"firstName\":\"Michael\",\"secondName\":\"A\",\"age\":\"29\"}",
      result("{\"firstName\":\"Michael\",\"secondName\":\"A\"}")
    )
  }

  @Disabled
  @Test def csvToJsonNullTest(): Unit = {
    val spark = createSpark("local[2]")
    import spark.implicits._
    val columns = Seq(
      DataColumn("firstName", isPK = true, DataType.StringType),
      DataColumn("secondName", isPK = true, DataType.StringType),
      DataColumn("age", isPK = false, dataType = IntegerType)
    )
    val result = new DataFrameOp(
      Seq(("Michael", "A", null)).toDF().toDF("firstName", "secondName", "age")
    ).csvToJSON(columns)
      .dataFrame()
      .collectAsList()
      .asScala
      .map(row => (row.getAs[String]("key"), row.getAs[String]("value")))
      .toMap

    assertEquals(1, result.size)
    assertEquals(
      "{\"firstName\":\"Michael\",\"secondName\":\"A\",\"age\":}",
      result("{\"firstName\":\"Michael\",\"secondName\":\"A\"}")
    )
  }

  @Test def jsonToByteTest(): Unit = {
    val spark = createSpark("local[2]")

    var data = Seq(Row(1, "A1", 52, "fghgh", "sfjojs", "zzz", "final", 5))
    (0 to 10000).iterator.foreach(_ =>
      data = data ++ Seq(Row(1, "A1", 52, "fghgh", "sfjojs", "zzz", "final", 5))
    )

    val structType = new StructType()
      .add("ID", "integer")
      .add("name", "string")
      .add("age", "integer")
      .add("xx", "string")
      .add("yy", "string")
      .add("zz", "string")
      .add("f", "string")
      .add("fInt", "integer")

    val columns = Seq(
      DataColumn("ID", isPK = true, dataType = IntegerType),
      DataColumn("name", isPK = false, dataType = StringType),
      DataColumn("age", isPK = false, dataType = IntegerType),
      DataColumn("xx", isPK = false, dataType = StringType),
      DataColumn("yy", isPK = false, dataType = StringType),
      DataColumn("zz", isPK = false, dataType = StringType),
      DataColumn("f", isPK = false, dataType = StringType),
      DataColumn("fInt", isPK = false, dataType = IntegerType)
    )

    val json = new DataFrameOp(
      spark.createDataFrame(spark.sparkContext.parallelize(data), structType)
    ).csvToJSON(columns)
      .dataFrame()
      .withColumn("byte", col("value").cast("Byte"))
      .selectExpr("CAST(byte AS BYTE)")
    val head = json.head()
    assertTrue(json.filter(_ != head).isEmpty)
  }
  def rows: List[List[String]] = {
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
}
