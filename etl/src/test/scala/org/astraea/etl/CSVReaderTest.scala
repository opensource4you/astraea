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

import com.opencsv.CSVWriter
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.astraea.etl.DataType.{IntegerType, StringType}
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

import java.io._
import java.nio.file.Files
import scala.util.{Failure, Random, Try}
import scala.collection.JavaConverters._

class CSVReaderTest {
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
    val tempPath = System.getProperty("java.io.tmpdir")
    val myDir = new File(tempPath + "/spark-" + Random.nextInt())
    myDir.mkdir()

    val file = Files.createTempFile(myDir.toPath, "local_kafka", ".csv")

    writeCsvFile(file.toAbsolutePath.toString, addPrefix(rows))

    val spark = CSVReader.createSpark("local[2]")

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

    assert(structType.length equals 4)

    val csvDF = CSVReader.readCSV(spark, structType, file.getParent.toString)

    assert(csvDF.isStreaming, "sessions must be a streaming Dataset")

    csvDF.writeStream
      .format("csv")
      .option("path", myDir.getPath + "/data")
      .option("checkpointLocation", myDir.getPath + "/checkpoint")
      .outputMode("append")
      .start()
      .awaitTermination(3000)

    spark.stop()

    val writeFile = getCSVFile(new File(myDir.getPath + "/data"))(0)
    val br = new BufferedReader(new FileReader(writeFile))

    assert(br.readLine equals "1,A1,52,fghgh")
    assert(br.readLine equals "2,B1,36,gjgbn")
    assert(br.readLine equals "3,C1,45,fgbhjf")
    assert(br.readLine equals "4,D1,25,dfjf")
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

  def addPrefix(lls: List[List[String]]): List[List[String]] =
    lls
      .foldLeft((1, List.empty[List[String]])) {
        case ((serial: Int, acc: List[List[String]]), value: List[String]) =>
          (serial + 1, (serial.toString +: value) +: acc)
      }
      ._2
      .reverse

  def writeCsvFile(
      path: String,
      rows: List[List[String]]
  ): Try[Unit] =
    Try(new CSVWriter(new BufferedWriter(new FileWriter(path)))).flatMap(
      (csvWriter: CSVWriter) =>
        Try {
          csvWriter.writeAll(
            rows.map(_.toArray).asJava
          )
          csvWriter.close()
        } match {
          case f @ Failure(_) =>
            Try(csvWriter.close()).recoverWith { case _ =>
              f
            }
          case success =>
            success
        }
    )

  def getCSVFile(file: File): Array[File] = {
    file
      .listFiles()
      .filter(!_.isDirectory)
      .filter(t => t.toString.endsWith(".csv"))
  }
}
