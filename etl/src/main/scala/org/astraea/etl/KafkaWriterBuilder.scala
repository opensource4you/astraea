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

import org.astraea.etl.KafkaWriterBuilder.{
  BootStrapStep,
  BuildStep,
  CheckpointStep,
  DFStep,
  FullWriter,
  TopicStep
}

class KafkaWriterBuilder[PassedStep <: BuildStep] private (
    var dataFrameOp: DataFrameOp,
    var bootstrap: String,
    var topic: String,
    var checkpoint: String
) {
  protected def this() =
    this(DataFrameOp.empty(), "bootStrap", "topic", "checkPoint")

  protected def this(pb: KafkaWriterBuilder[_]) = this(
    pb.dataFrameOp,
    pb.bootstrap,
    pb.topic,
    pb.checkpoint
  )

  def dataFrameOp(
      dataFrameOp: DataFrameOp
  ): KafkaWriterBuilder[PassedStep with DFStep] = {
    this.dataFrameOp = dataFrameOp
    new KafkaWriterBuilder[PassedStep with DFStep](this)
  }

  def bootstrapServer(
      bootstrap: String
  ): KafkaWriterBuilder[PassedStep with BootStrapStep] = {
    this.bootstrap = bootstrap
    new KafkaWriterBuilder[PassedStep with BootStrapStep](this)
  }

  def topic(topic: String): KafkaWriterBuilder[PassedStep with TopicStep] = {
    this.topic = topic
    new KafkaWriterBuilder[PassedStep with TopicStep](this)
  }

  def checkpoint(
      checkpoint: String
  ): KafkaWriterBuilder[PassedStep with CheckpointStep] = {
    this.checkpoint = checkpoint
    new KafkaWriterBuilder[PassedStep with CheckpointStep](this)
  }

  def build()(implicit ev: PassedStep =:= FullWriter): KafkaWriter = {
    KafkaWriter(dataFrameOp, bootstrap, topic, checkpoint)
  }
}

object KafkaWriterBuilder {
  sealed trait BuildStep
  sealed trait DFStep extends BuildStep
  sealed trait BootStrapStep extends BuildStep
  sealed trait TopicStep extends BuildStep
  sealed trait CheckpointStep extends BuildStep

  type FullWriter = DFStep with BootStrapStep with TopicStep with CheckpointStep

  def builder() = new KafkaWriterBuilder[BuildStep]()
}
