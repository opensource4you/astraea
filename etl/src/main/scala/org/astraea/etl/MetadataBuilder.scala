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

import java.io.File

class MetadataBuilder private (
    var deploymentModel: String,
    var sourcePath: File,
    var sinkPath: File,
    var column: Map[String, DataType],
    var primaryKeys: Map[String, DataType],
    var kafkaBootstrapServers: String,
    var topicName: String,
    var numPartitions: Int,
    var numReplicas: Short,
    var topicConfig: Map[String, String]
) {
  protected def this() = this(
    "deploymentModel",
    new File(""),
    new File(""),
    Map.empty,
    Map.empty,
    "kafkaBootstrapServers",
    "topicName",
    -1,
    -1,
    Map.empty
  )

  protected def this(pb: MetadataBuilder) = this(
    pb.deploymentModel,
    pb.sourcePath,
    pb.sinkPath,
    pb.column,
    pb.primaryKeys,
    pb.kafkaBootstrapServers,
    pb.topicName,
    pb.numPartitions,
    pb.numReplicas,
    pb.topicConfig
  )

  def deploymentMode(str: String): MetadataBuilder = {
    this.deploymentModel = str
    new MetadataBuilder(this)
  }

  def sourcePath(file: File): MetadataBuilder = {
    this.sourcePath = file
    new MetadataBuilder(this)
  }

  def sinkPath(file: File): MetadataBuilder = {
    this.sinkPath = file
    new MetadataBuilder(this)
  }

  def columns(map: Map[String, DataType]): MetadataBuilder = {
    this.column = map
    new MetadataBuilder(this)
  }

  def primaryKey(map: Map[String, DataType]): MetadataBuilder = {
    this.primaryKeys = map
    new MetadataBuilder(this)
  }

  def kafkaBootstrapServers(str: String): MetadataBuilder = {
    this.kafkaBootstrapServers = str
    new MetadataBuilder(this)
  }

  def topicName(str: String): MetadataBuilder = {
    this.topicName = str
    new MetadataBuilder(this)
  }

  def numPartitions(num: Int): MetadataBuilder = {
    this.numPartitions = num
    new MetadataBuilder(this)
  }

  def numReplicas(num: Short): MetadataBuilder = {
    this.numReplicas = num
    new MetadataBuilder(this)
  }

  def topicConfig(map: Map[String, String]): MetadataBuilder = {
    this.topicConfig = map
    new MetadataBuilder(this)
  }

  def build(): Metadata = {
    Metadata(
      this.deploymentModel,
      this.sourcePath,
      this.sinkPath,
      this.column,
      this.primaryKeys,
      this.kafkaBootstrapServers,
      this.topicName,
      this.numPartitions,
      this.numReplicas,
      this.topicConfig
    )
  }
}

object MetadataBuilder {
  def of(): MetadataBuilder = {
    new MetadataBuilder()
  }
}
