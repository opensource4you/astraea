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

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.astraea.etl.Writer._

class Writer[PassedStep <: BuildStep] private (
    var dataFrameOp: DataFrameOp,
    var target: String,
    var checkpoint: String
) {
  protected def this() =
    this(DataFrameOp.empty(), "topic", "checkPoint")

  protected def this(pb: Writer[_]) = this(
    pb.dataFrameOp,
    pb.target,
    pb.checkpoint
  )

  def dataFrameOp(
      dataFrameOp: DataFrameOp
  ): Writer[PassedStep with DFStep] = {
    this.dataFrameOp = dataFrameOp
    new Writer[PassedStep with DFStep](this)
  }

  def writeToKafka(
      bootstrap: String
  )(implicit ev: PassedStep =:= FullWriter): DataStreamWriter[Row] = {
    dataFrameOp
      .dataFrame()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode(OutputMode.Append())
      .format("kafka")
      .option(
        "kafka." +
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        bootstrap
      )
      // Spark to kafka transfer support for StringSerializer and ByteSerializer in spark 3.3.0 .
      .option(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      .option(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      .option("topic", target)
      .option(ProducerConfig.ACKS_CONFIG, "all")
      .option(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      .option("checkpointLocation", checkpoint)
  }

  /** Target represents the destination of the data, for Kafka it represents the
    * topic, for other targets it can represent the data path.
    *
    * @param target
    *   destination of the data
    * @return
    *   Writer
    */
  def target(target: String): Writer[PassedStep with TargetStep] = {
    this.target = target
    new Writer[PassedStep with TargetStep](this)
  }

  def checkpoint(
      checkpoint: String
  ): Writer[PassedStep with CheckpointStep] = {
    this.checkpoint = checkpoint
    new Writer[PassedStep with CheckpointStep](this)
  }
}

object Writer {
  sealed trait BuildStep
  sealed trait DFStep extends BuildStep
  sealed trait TargetStep extends BuildStep
  sealed trait CheckpointStep extends BuildStep

  type FullWriter = DFStep with TargetStep with CheckpointStep

  def of() = new Writer[BuildStep]()
}
