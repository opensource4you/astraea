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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types.{StructField, StructType}
abstract class OptionalDataFrameBuilder(sparkSession: SparkSession) {
  private val SOURCE_ARCHIVE_DIR = "sourceArchiveDir"
  private val CLEAN_SOURCE = "cleanSource"
  private val RECURSIVE_FILE_LOOK_UP = "recursiveFileLookup"

  private var _recursiveFileLookup: String = "true"
  private var _cleanSource: String = "delete"
  private var _source: String = ""
  private var _sourceArchiveDir: String = ""
  private var _columns: Seq[DataColumn] = Seq.empty

  def recursiveFileLookup(r: String): OptionalDataFrameBuilder = {
    _recursiveFileLookup = r
    this
  }

  def archive(c: String): OptionalDataFrameBuilder = {
    _cleanSource = c
    this
  }

  def source(s: String): OptionalDataFrameBuilder = {
    _source = s
    this
  }

  def columns(c: Seq[DataColumn]): OptionalDataFrameBuilder = {
    _columns = c
    this
  }

  def sourceArchiveDir(a: String): OptionalDataFrameBuilder = {
    _sourceArchiveDir = a
    this
  }

  def build(): OptionalDataFrame = {
    val df = options(sparkSession.readStream)
      .option("recursiveFileLookup", _recursiveFileLookup)
      .option("cleanSource", _cleanSource)
      .option("sourceArchiveDir", _sourceArchiveDir)
      .schema(OptionalDataFrameBuilder.schema(_columns))
      .csv(_source)
      .filter(row => {
        val bool = (0 until row.length).exists(i => !row.isNullAt(i))
        bool
      })

    new OptionalDataFrame(df)
  }

  private def options(reader: DataStreamReader): DataStreamReader = {
    val r = reader
    if (_source.isBlank)
      throw new IllegalArgumentException(
        s"Source is blank, you must configure source in spark2kafka config"
      )
    if (_cleanSource == "archive") {
      if (_sourceArchiveDir.isBlank)
        throw new IllegalArgumentException(
          s"$SOURCE_ARCHIVE_DIR is blank.When you set cleanSource to 'archive', you must configure ArchiveDir in spark2kafka config"
        )
      r.option(CLEAN_SOURCE, _cleanSource)
        .option(SOURCE_ARCHIVE_DIR, _sourceArchiveDir)
        .option(RECURSIVE_FILE_LOOK_UP, _recursiveFileLookup)
    } else {
      r.option(CLEAN_SOURCE, _cleanSource)
        .option(RECURSIVE_FILE_LOOK_UP, _recursiveFileLookup)
    }
    r
  }
}

object OptionalDataFrameBuilder {
  def builder(): OptionalDataFrameBuilder = OptionalDataFrameBuilderImpl(
    SparkSession
      .builder()
      .appName("AstraeaETL")
      .getOrCreate()
  )

  // for testing
  def builder(sparkSession: SparkSession): OptionalDataFrameBuilder =
    OptionalDataFrameBuilderImpl(sparkSession)

  private case class OptionalDataFrameBuilderImpl(
      private val sparkSession: SparkSession
  ) extends OptionalDataFrameBuilder(sparkSession)

  private def schema(columns: Seq[DataColumn]): StructType =
    StructType(columns.map { col =>
      if (col.dataType != DataType.StringType)
        throw new IllegalArgumentException(
          "Sorry, only string type is currently supported.Because a problem(astraea #1286) has led to the need to wrap the non-nullable type."
        )
      StructField(col.name, col.dataType.sparkType)
    })
}
