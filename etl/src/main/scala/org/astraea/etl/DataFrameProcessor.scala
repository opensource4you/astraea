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

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.astraea.common.json.JsonConverter

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

class DataFrameProcessor(dataFrame: DataFrame) {

  val defaultConverter: UserDefinedFunction =
    udf[String, Map[String, String]]((value: Map[String, String]) => {
      JsonConverter
        .jackson()
        .toJson(
          value.asJava
        )
    })

  /** Turn the original DataFrame into a key-value table.Integrate all columns
    * into one value->josh. If there are multiple primary keys, key will become
    * keyA_keyB_....
    *
    * {{{
    * Seq(Person("Michael","A.K", 29), Person("Andy","B.C", 30), Person("Justin","C.L", 19))
    *
    * Person
    * |-- FirstName: string
    * |-- SecondName: string
    * |-- Age: Integer
    *
    * Key:FirstName,SecondName
    *
    * // +-----------+---------------------------------------------------+
    * // |        key|                                              value|
    * // +-----------+---------------------------------------------------+
    * // |Michael,A.K|{"FirstName":"Michael","SecondName":"A.K","Age":29}|
    * // |   Andy,B.C|{"FirstName":"Andy","SecondName":"B.C","Age":30}   |
    * // | Justin,C.L|{"FirstName":"Justin","SecondName":"C.L","Age":19} |
    * // +-----------+---------------------------------------------------+
    * }}}
    *
    * @param cols
    *   cols metadata
    * @return
    *   json df
    */
  def csvToJSON(cols: Seq[DataColumn]): DataFrameProcessor = {
    new DataFrameProcessor(
      dataFrame
        .withColumn(
          "value",
          defaultConverter(
            map(cols.flatMap(c => List(lit(c.name), col(c.name))): _*)
          )
        )
        .withColumn(
          "key",
          defaultConverter(
            map(
              cols
                .filter(dataColumn => dataColumn.isPk)
                .flatMap(c => List(lit(c.name), col(c.name))): _*
            )
          )
        )
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    )
  }

  def dataFrame(): DataFrame = {
    dataFrame
  }

  def toKafkaWriterBuilder(metadata: Metadata): SparkStreamWriter[Row] = {
    SparkStreamWriter.writeToKafka(this, metadata)
  }
}

object DataFrameProcessor {
  def builder(sparkSession: SparkSession) = new Builder(sparkSession)
  def fromLocalCsv(
      sparkSession: SparkSession,
      metadata: Metadata
  ): DataFrameProcessor = {
    builder(sparkSession)
      .source(metadata.sourcePath)
      .columns(metadata.columns)
      .cleanSource(metadata.cleanSource)
      .recursiveFileLookup(metadata.recursiveFile)
      .sourceArchiveDir(metadata.archivePath)
      .buildFromCsv()
  }
  class Builder(
      private val sparkSession: SparkSession
  ) {
    private val SOURCE_ARCHIVE_DIR = "sourceArchiveDir"
    private val CLEAN_SOURCE = "cleanSource"
    private val RECURSIVE_FILE_LOOK_UP = "recursiveFileLookup"

    private var _recursiveFileLookup: String = ""
    private var _cleanSource: String = ""
    private var _source: String = ""
    private var _sourceArchiveDir: String = ""
    private var _columns: Seq[DataColumn] = Seq.empty

    def recursiveFileLookup(r: String): Builder = {
      _recursiveFileLookup = r
      this
    }

    def cleanSource(c: String): Builder = {
      _cleanSource = c
      this
    }

    def source(s: String): Builder = {
      _source = s
      this
    }

    def columns(c: Seq[DataColumn]): Builder = {
      _columns = c
      this
    }

    def sourceArchiveDir(a: String): Builder = {
      _sourceArchiveDir = a
      this
    }

    def buildFromCsv(): DataFrameProcessor = {
      if (_cleanSource == "archive") {
        if (_sourceArchiveDir.isBlank)
          throw new IllegalArgumentException(
            s"$SOURCE_ARCHIVE_DIR is blank.When you set cleanSource to 'archive', you must configure ArchiveDir in spark2kafka config"
          )
      }

      val df = sparkSession.readStream
        .option(CLEAN_SOURCE, _cleanSource)
        .option(SOURCE_ARCHIVE_DIR, _sourceArchiveDir)
        .option(RECURSIVE_FILE_LOOK_UP, _recursiveFileLookup)
        .schema(schema(_columns))
        .csv(_source)
        .filter(row => {
          val bool = (0 until row.length).exists(i => !row.isNullAt(i))
          bool
        })
      new DataFrameProcessor(df)
    }

    private def schema(columns: Seq[DataColumn]): StructType =
      StructType(columns.map { col =>
        if (col.dataType != DataType.StringType)
          throw new IllegalArgumentException(
            "Sorry, only string type is currently supported.Because a problem(astraea #1286) has led to the need to wrap the non-nullable type."
          )
        StructField(col.name, col.dataType.sparkType)
      })
  }
}
