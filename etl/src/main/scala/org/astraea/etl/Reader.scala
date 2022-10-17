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

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.astraea.etl.Reader.createSpark

case class Reader(
    deploymentModel: String,
    userSchema: StructType,
    sourcePath: String,
    sinkPath: String
) {
  def read(): DataFrameOp = {
    new DataFrameOp(
      createSpark(deploymentModel).readStream
        .option("cleanSource", "archive")
        .option("sourceArchiveDir", sinkPath)
        .schema(userSchema)
        .csv(sourcePath)
    )
  }
}

object Reader {

  def createSpark(deploymentModel: String): SparkSession = {
    SparkSession
      .builder()
      .master(deploymentModel)
      .appName("Astraea ETL")
      .getOrCreate()
  }

  def createSchema(
      cols: Map[String, DataType],
      pk: Map[String, DataType]
  ): StructType = {
    var userSchema = new StructType()
    cols.foreach(col =>
      if (pk.contains(col._1))
        userSchema = userSchema.add(col._1, col._2.value, nullable = false)
      else
        userSchema = userSchema.add(col._1, col._2.value, nullable = true)
    )
    userSchema
  }

  def read(
      spark: SparkSession,
      userSchema: StructType,
      sourcePath: String,
      sinkPath: String
  ): DataFrame = {
    spark.readStream
      .option("cleanSource", "archive")
      .option("sourceArchiveDir", sinkPath)
      .schema(userSchema)
      .csv(sourcePath)
  }
}
