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
import org.astraea.etl.DataType.StringType
import org.astraea.etl.CSVReaderBuilder._
class CSVReaderBuilder[PassedStep <: BuildStep] private (
    var deploymentModel: String,
    var userSchema: StructType,
    var sourcePath: String,
    var sinkPath: String
) {
  protected def this() = this(
    "deploymentModel",
    CSVReader
      .createSchema(Map("Type" -> StringType), Map("Type" -> StringType)),
    "sourcePath",
    "sinkPath"
  )

  protected def this(pb: CSVReaderBuilder[_]) = this(
    pb.deploymentModel,
    pb.userSchema,
    pb.sourcePath,
    pb.sinkPath
  )

  def spark(
      deploymentModel: String
  ): CSVReaderBuilder[PassedStep with SparkStep] = {
    this.deploymentModel = deploymentModel
    new CSVReaderBuilder[PassedStep with SparkStep](this)
  }

  def schema(
      userSchema: StructType
  ): CSVReaderBuilder[PassedStep with SchemaStep] = {
    this.userSchema = userSchema
    new CSVReaderBuilder[PassedStep with SchemaStep](this)
  }

  def sourcePath(
      source: String
  ): CSVReaderBuilder[PassedStep with SourceStep] = {
    this.sourcePath = source
    new CSVReaderBuilder[PassedStep with SourceStep](this)
  }

  def sinkPath(sink: String): CSVReaderBuilder[PassedStep with SinkStep] = {
    this.sinkPath = sink
    new CSVReaderBuilder[PassedStep with SinkStep](this)
  }

  def build()(implicit ev: PassedStep =:= FullReader): CSVReader = {
    CSVReader(deploymentModel, userSchema, sourcePath, sinkPath)
  }

}

object CSVReaderBuilder {
  sealed trait BuildStep
  sealed trait SparkStep extends BuildStep
  sealed trait SchemaStep extends BuildStep
  sealed trait SourceStep extends BuildStep
  sealed trait SinkStep extends BuildStep

  type FullReader = SparkStep with SchemaStep with SourceStep with SinkStep
  def builder() = new CSVReaderBuilder[BuildStep]()
}
