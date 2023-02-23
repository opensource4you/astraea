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

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}
object FileCreator {
  def generateCSVF(
      sourceDir: Path,
      rows: List[List[String]]
  ): Future[Boolean] = {
    Future { generateCSV(sourceDir, rows) }
  }

  def generateCSV(sourceDir: Path, rows: List[List[String]]): Boolean = {
    createCSV(sourceDir, rows, 0)
    Thread.sleep(Duration(12, TimeUnit.SECONDS).toMillis)
    createCSV(sourceDir, rows, 1)
    true
  }

  def createCSV(
      sourceDir: Path,
      rows: List[List[String]],
      int: Int
  ): Try[Unit] = {
    val str =
      sourceDir.toAbsolutePath.toString + "/local_kafka" + "-" + int + ".csv"
    val fileCSV2 = Files.createFile(Path.of(str))
    writeCsvFile(fileCSV2.toAbsolutePath.toString, rows)
  }

  def writeCsvFile(
      path: String,
      rows: List[List[String]]
  ): Try[Unit] =
    Try(new CSVWriter(new BufferedWriter(new FileWriter(path)))).flatMap(
      (csvWriter: CSVWriter) =>
        Try {
          csvWriter.writeAll(rows.map(_.toArray).asJava)
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

  def getCSVFile(file: Path): Seq[Path] = {
    Files
      .list(file)
      .filter(f => Files.isRegularFile(f))
      .filter(f => f.getFileName.toString.endsWith(".csv"))
      .iterator()
      .asScala
      .toSeq
  }
}
