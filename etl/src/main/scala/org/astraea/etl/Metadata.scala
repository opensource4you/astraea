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
import java.nio.file.Files
import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Parameters required for Astraea ETL.
  *
  * @param sourcePath
  *   The data source path should be a directory.
  * @param columns
  *   The CSV Column metadata.Contains column name, data type and is pk or not.
  * @param kafkaBootstrapServers
  *   The Kafka bootstrap servers.
  * @param topicName
  *   Set your topic name, if it is empty that will set it to "'spark'-'current
  *   time by hourDay'-'random number'".
  * @param numberOfPartitions
  *   Set the number of topic partitions, if it is empty that will set it to 15.
  * @param numberOfReplicas
  *   Set the number of topic replicas, if it is empty that will set it to 3.
  * @param checkpoint
  *   Spark checkpoint path.
  */
case class Metadata private (
    sourcePath: String,
    checkpoint: String,
    columns: Seq[DataColumn],
    kafkaBootstrapServers: String,
    topicName: String,
    topicConfigs: Map[String, String],
    numberOfPartitions: Int,
    numberOfReplicas: Short
)

object Metadata {
  private[etl] val SOURCE_PATH_KEY = "source.path"
  private[etl] val CHECKPOINT_KEY = "checkpoint"
  private[etl] val COLUMN_NAME_KEY = "column.names"
  private[etl] val COLUMN_TYPES_KEY = "column.types"
  private[etl] val PRIMARY_KEY_KEY = "primary.keys"
  private[etl] val KAFKA_BOOTSTRAP_SERVERS_KEY = "kafka.bootstrap.servers"
  private[etl] val TOPIC_NAME_KEY = "topic.name"
  private[etl] val TOPIC_CONFIGS_KEY = "topic.configs"
  private[etl] val TOPIC_PARTITIONS_KEY = "topic.partitions"
  private[etl] val TOPIC_REPLICAS_KEY = "topic.replicas"

  private[etl] val DEFAULT_PARTITIONS = 15
  private[etl] val DEFAULT_REPLICAS = 1.toShort

  // Parameters needed to configure ETL.
  def of(path: File): Metadata = {
    // remove the empty/blank value
    val properties =
      readProp(path).asScala.filter(_._2.nonEmpty).filterNot(_._2.isBlank)
    val columnNames = properties(COLUMN_NAME_KEY).split(",").toSeq
    if (columnNames.isEmpty)
      throw new IllegalArgumentException("columns must be defined")
    val pks = properties
      .get(PRIMARY_KEY_KEY)
      .map(_.split(",").toSet)
      .getOrElse(columnNames.toSet)
    val types = properties
      .get(COLUMN_TYPES_KEY)
      .map(s =>
        s.split(",")
          .map(t => DataType.of(t))
          .toSeq
      )
      .getOrElse(Seq.empty)
    Metadata(
      sourcePath = properties(SOURCE_PATH_KEY),
      checkpoint = properties.getOrElse(
        CHECKPOINT_KEY,
        Files.createTempDirectory("astraea-etl").toString
      ),
      columns = columnNames.zipWithIndex.map { case (c, index) =>
        DataColumn(
          name = c,
          isPk = pks.contains(c),
          dataType = if (types.isEmpty) DataType.StringType else types(index)
        )
      },
      kafkaBootstrapServers = properties(KAFKA_BOOTSTRAP_SERVERS_KEY),
      topicName =
        properties.getOrElse(TOPIC_NAME_KEY, "astraea-etl-" + Math.random()),
      topicConfigs = properties
        .get(TOPIC_CONFIGS_KEY)
        .map(s =>
          s.split(",")
            .map(item => item.split("=")(0) -> item.split("=")(1))
            .toMap
        )
        .getOrElse(Map.empty),
      numberOfPartitions = properties
        .get(TOPIC_PARTITIONS_KEY)
        .map(_.toInt)
        .getOrElse(DEFAULT_PARTITIONS),
      numberOfReplicas = properties
        .get(TOPIC_REPLICAS_KEY)
        .map(_.toShort)
        .getOrElse(DEFAULT_REPLICAS)
    )
  }

  private[this] def readProp(path: File): Properties = {
    val properties = new Properties()
    Using(scala.io.Source.fromFile(path)) { bufferedSource =>
      properties.load(bufferedSource.reader())
    }
    properties
  }
}
