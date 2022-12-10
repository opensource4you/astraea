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

import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.astraea.etl.DataType.StringType
import org.astraea.etl.Reader._

import scala.util.control.Breaks
class Reader[PassedStep <: BuildStep] private (
    var deploymentModel: String,
    var userSchema: StructType,
    var sinkPath: String,
    var pk: Seq[String]
) extends Serializable {
  protected def this() = this(
    "deploymentModel",
    Reader
      .createSchema(Map("Type" -> StringType)),
    "sinkPath",
    Seq.empty
  )

  protected def this(pb: Reader[_]) = this(
    pb.deploymentModel,
    pb.userSchema,
    pb.sinkPath,
    pb.pk
  )

  def spark(
      deploymentModel: String
  ): Reader[PassedStep with SparkStep] = {
    this.deploymentModel = deploymentModel
    new Reader[PassedStep with SparkStep](this)
  }

  def schema(
      userSchema: StructType
  ): Reader[PassedStep with SchemaStep] = {
    this.userSchema = userSchema
    new Reader[PassedStep with SchemaStep](this)
  }

  def sinkPath(sink: String): Reader[PassedStep with SinkStep] = {
    this.sinkPath = sink
    new Reader[PassedStep with SinkStep](this)
  }

  def primaryKeys(pk: Seq[String]): Reader[PassedStep with PkStep] = {
    this.pk = pk
    new Reader[PassedStep with PkStep](this)
  }
  // check blank line
  def allNull(row: Row): Boolean = {
    @transient val len = row.length
    @transient var i = 0
    @transient var isNull = true
    @transient val loop = new Breaks
    loop.breakable {
      while (i < len) {
        if (!row.isNullAt(i)) {
          isNull = false
          loop.break
        }
        i += 1
      }
    }
    isNull
  }

  def readCSV(
      source: String,
      @transient blankLine: Boolean
  )(implicit ev: PassedStep =:= FullReader): DataFrameOp = {
    var df = createSpark(deploymentModel).readStream
      .option("cleanSource", "archive")
      .option("sourceArchiveDir", sinkPath)
      .schema(userSchema)
      .csv(source)

    pk.foreach(str =>
      df = df
        .filter(row => {
          blankLine && (!allNull(row))
        })
        .withColumn(str, new Column(AssertNotNull(col(str).expr)))
    )
    new DataFrameOp(df)
  }

}

object Reader {
  sealed trait BuildStep
  sealed trait SparkStep extends BuildStep
  sealed trait SchemaStep extends BuildStep
  sealed trait SinkStep extends BuildStep
  sealed trait PkStep extends BuildStep

  type FullReader = SparkStep with SchemaStep with SinkStep with PkStep
  def of() = new Reader[BuildStep]()

  def createSpark(deploymentModel: String): SparkSession = {
    SparkSession
      .builder()
      .master(deploymentModel)
      .appName("Astraea ETL")
      .getOrCreate()
  }

  def createSchema(cols: Map[String, DataType]): StructType = {
    var userSchema = new StructType()
    cols.foreach(col => userSchema = userSchema.add(col._1, col._2.value))
    userSchema
  }
}
