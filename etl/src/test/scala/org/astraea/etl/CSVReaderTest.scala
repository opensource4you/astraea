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

import org.apache.spark.SparkException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, Row}
import org.astraea.etl.CSVReader.{createSpark, csvToJSON}
import org.astraea.etl.DataType.{IntegerType, StringType}
import org.astraea.etl.FileCreator.{addPrefix, getCSVFile, writeCsvFile}
import org.astraea.it.RequireBrokerCluster
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.io._
import java.nio.file.Files
import scala.util.Random

class CSVReaderTest extends RequireBrokerCluster {
  @Test def createSchemaNullTest(): Unit = {
    val spark = CSVReader.createSpark("local[2]")
    val seq = Seq(
      Row(1, "A1", 52, "fghgh"),
      Row(2, "B2", 42, "affrgg"),
      Row(3, "C3", 55, "safdfg"),
      Row(null, "D4", 59, "rehrth")
    )

    val structType = CSVReader.createSchema(
      Map(
        "SerialNumber" -> IntegerType,
        "RecordNumber" -> StringType,
        "Size" -> IntegerType,
        "Type" -> StringType
      ),
      Map(
        "SerialNumber" -> IntegerType
      )
    )
    val df =
      spark.createDataFrame(spark.sparkContext.parallelize(seq), structType)

    assertThrows(classOf[SparkException], () => df.show())
  }

  @Test def sparkReadCSVTest(): Unit = {
    val tempPath =
      System.getProperty("java.io.tmpdir") + "/sparkFile" + Random.nextInt()
    val myDir = new File(tempPath)
    myDir.mkdir()

    val sourceDir = new File(myDir.getPath + "/source")
    sourceDir.mkdir()

    val fileCSV = new File(sourceDir.toPath + "/local_kafka.csv")
    writeCsvFile(fileCSV.getPath, addPrefix(rows))

    val sinkDir = new File(tempPath + "/sink")
    sinkDir.mkdir()

    val checkoutDir = new File(tempPath + "/checkout")
    checkoutDir.mkdir()

    val dataDir = new File(tempPath + "/data")
    dataDir.mkdir()

    val spark = CSVReader.createSpark("local[5]")

    val structType = CSVReader.createSchema(
      Map(
        "SerialNumber" -> IntegerType,
        "RecordNumber" -> StringType,
        "Size" -> IntegerType,
        "Type" -> StringType
      ),
      Map(
        "SerialNumber" -> IntegerType
      )
    )

    assertEquals(structType.length, 4)
    val csvDF =
      CSVReader.readCSV(spark, structType, sourceDir.getPath, sinkDir.getPath)

    assertTrue(csvDF.isStreaming, "sessions must be a streaming Dataset")

    csvDF.writeStream
      .format("csv")
      .option("path", dataDir.getPath)
      .option("checkpointLocation", checkoutDir.getPath)
      .outputMode("append")
      .start()
      .awaitTermination(5000)

    val writeFile = getCSVFile(new File(dataDir.getPath))(0)
    val br = new BufferedReader(new FileReader(writeFile))

    assertEquals(br.readLine, "1,A1,52,fghgh")
    assertEquals(br.readLine, "2,B1,36,gjgbn")
    assertEquals(br.readLine, "3,C1,45,fgbhjf")
    assertEquals(br.readLine, "4,D1,25,dfjf")
  }

  @Test def csvToJSONTest(): Unit = {
    val spark = createSpark("local[2]")
    import spark.implicits._

    case class Person(name: String, age: Long)
    val data =
      Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 19))
    implicit val make: Encoder[Person] =
      org.apache.spark.sql.Encoders.kryo[Person]
    val df =
      spark.createDataset(data).map(x => (x.name, x.age)).toDF("name", "age")

    val json = csvToJSON(df, Seq("name"))
    val iterator = (0 to 2).iterator
    json
      .collectAsList()
      .forEach(row => {
        val i = iterator.next()
        assertEquals(row(0), data(i).name)
        assertEquals(
          row(1),
          s"""{"name":"${data(i).name}","age":${data(i).age}}"""
        )
      })
  }

  @Test def csvToJsonMulKeysTest(): Unit = {
    val spark = createSpark("local[2]")
    import spark.implicits._

    case class Person(firstName: String, secondName: String, age: Long)
    val data =
      Seq(
        Person("Michael", "A", 29),
        Person("Andy", "B", 30),
        Person("Justin", "C", 19)
      )
    implicit val make: Encoder[Person] =
      org.apache.spark.sql.Encoders.kryo[Person]
    val df =
      spark
        .createDataset(data)
        .map(x => (x.firstName, x.secondName, x.age))
        .toDF("firstName", "secondName", "age")

    val json = csvToJSON(
      df,
      Seq("firstName", "secondName")
    )
    val iterator = (0 to 2).iterator
    json
      .collectAsList()
      .forEach(row => {
        val i = iterator.next()
        assertEquals(row(0), s"${data(i).firstName},${data(i).secondName}")
        assertEquals(
          row(1),
          s"""{"firstName":"${data(
            i
          ).firstName}","secondName":"${data(i).secondName}","age":${data(
            i
          ).age}}"""
        )
      })
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

    val df =
      spark.createDataFrame(spark.sparkContext.parallelize(data), structType)

    val json = csvToJSON(df, Seq("ID"))
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
