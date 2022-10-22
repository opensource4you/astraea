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

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Files
import java.util.concurrent.{Executor, Executors, TimeUnit}
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}
object FileCreator {
  def generateCSVF(
      sourceDir: File,
      rows: List[List[String]]
  ): Future[Boolean] = {
    val singleThread: Executor = Executors.newSingleThreadExecutor()
    implicit val singleThreadExecutionContext: ExecutionContext =
      ExecutionContext.fromExecutor(singleThread)
    Future { generateCSV(sourceDir, rows) }
  }

  def generateCSV(sourceDir: File, rows: List[List[String]]): Boolean = {
    Range
      .inclusive(0, 5)
      .foreach(i => {
        createCSV(sourceDir, rows, i)
        Thread.sleep(Duration(2, TimeUnit.SECONDS).toMillis)
      })
    true
  }

  def createCSV(
      sourceDir: File,
      rows: List[List[String]],
      int: Int
  ): Try[Unit] = {
    val str = sourceDir + "/local_kafka" + "-" + int.toString + ".csv"
    val fileCSV2 = Files.createFile(new File(str).toPath)
    writeCsvFile(fileCSV2.toAbsolutePath.toString, addPrefix(rows))
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
            rows.map(_.toArray).asJava,
            true
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

  def mkdir(string: String): File = {
    val mkdirFile = new File(string)
    mkdirFile.mkdir()
    mkdirFile
  }
}
