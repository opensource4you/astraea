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
import org.apache.spark.sql.types.{StructField, StructType}

object ReadStreams {

  def create(source: String, columns: Seq[DataColumn]): DataFrameOp = create(
    SparkSession
      .builder()
      .appName("Astraea ETL")
      .getOrCreate(),
    source,
    columns
  )

  def create(
      session: SparkSession,
      source: String,
      columns: Seq[DataColumn]
  ): DataFrameOp = {
    val df = session.readStream
      .option("cleanSource", "delete")
      .schema(schema(columns))
      .csv(source)
      .filter(row => {
        val bool = (0 until row.length).exists(i => !row.isNullAt(i))
        bool
      })

    new DataFrameOp(df)
  }

  def schema(columns: Seq[DataColumn]): StructType =
    StructType(columns.map { col =>
      if (col.dataType != DataType.StringType)
        throw new IllegalArgumentException(
          "Sorry, only string type is currently supported.Because a problem(astraea #1286) has led to the need to wrap the non-nullable type."
        )
      StructField(col.name, col.dataType.sparkType)
    })

}
