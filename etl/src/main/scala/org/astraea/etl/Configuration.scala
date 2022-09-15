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
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Parameters required for Astraea ETL.
  *
  * @param sourcePath
  *   The data source path should be a directory.
  * @param sinkPath
  *   The data sink path should be a directory.
  * @param columnName
  *   The CSV Column Name.For example:stringA,stringB,stringC...
  * @param primaryKeys
  *   Primary keys.
  * @param kafkaBootstrapServers
  *   The Kafka bootstrap servers.
  * @param topicName
  *   Set your topic name, if it is empty that will set it to "'spark'-'current
  *   time by hourDay'-'random number'".
  * @param numPartitions
  *   Set the number of topic partitions, if it is empty that will set it to 15.
  * @param numReplicas
  *   Set the number of topic replicas, if it is empty that will set it to 3.
  * @param topicConfig
  *   The rest of the topic can be configured parameters.For example:
  *   keyA:valueA,keyB:valueB,keyC:valueC...
  */
case class Configuration(
    sourcePath: File,
    sinkPath: File,
    columnName: Array[String],
    primaryKeys: Array[String],
    kafkaBootstrapServers: String,
    topicName: String,
    numPartitions: Int,
    numReplicas: Int,
    topicConfig: Map[String, String]
)

object Configuration {
  private[this] val SOURCE_PATH = "source.path"
  private[this] val SINK_PATH = "sink.path"
  private[this] val COLUMN_NAME = "column.name"
  private[this] val PRIMARY_KEYS = "primary.keys"
  private[this] val KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"
  private[this] val TOPIC_NAME = "topic.name"
  private[this] val TOPIC_PARTITIONS = "topic.partitions"
  private[this] val TOPIC_REPLICAS = "topic.replicas"
  private[this] val TOPIC_CONFIG = "topic.config"

  private[this] val DEFAULT_PARTITIONS = 15
  private[this] val DEFAULT_REPLICAS = 1

  //Parameters needed to configure ETL.
  def apply(path: File): Configuration = {
    val properties = readProp(path).asScala.filter(_._2.nonEmpty).toMap

    val sourcePath = Utils.requireFolder(
      properties.getOrElse(
        SOURCE_PATH,
        throw new NullPointerException(
          SOURCE_PATH + "is null." + "You must configure " + SOURCE_PATH
        )
      )
    )
    val sinkPath = Utils.requireFolder(
      properties.getOrElse(
        SINK_PATH,
        throw new NullPointerException(
          SINK_PATH + "is null." + "You must configure " + SINK_PATH
        )
      )
    )
    val column = requireNonidentical(COLUMN_NAME, properties)
    val pKeys = primaryKeys(properties, column)
    //TODO check the format after linking Kafka
    val bootstrapServer = properties(KAFKA_BOOTSTRAP_SERVERS)
    val topicName = properties.getOrElse(
      TOPIC_NAME,
      throw new NullPointerException(
        TOPIC_NAME + "is null." + "You must configure " + TOPIC_NAME
      )
    )
    val topicPartitions = properties
      .get(TOPIC_PARTITIONS)
      .map(_.toInt)
      .getOrElse(DEFAULT_PARTITIONS)
    val topicReplicas = properties
      .get(TOPIC_REPLICAS)
      .map(_.toInt)
      .getOrElse(DEFAULT_REPLICAS)
    val topicConfig = topicParameters(properties.getOrElse(TOPIC_CONFIG, ""))

    Configuration(
      sourcePath,
      sinkPath,
      column,
      pKeys,
      bootstrapServer,
      topicName,
      topicPartitions,
      topicReplicas,
      topicConfig
    )
  }

  //Handling the topic.parameters parameter.
  def topicParameters(
      topicParameters: String
  ): Map[String, String] = {
    if (topicParameters.nonEmpty) {
      val parameters = topicParameters.split(",")
      var paramArray: ArrayBuffer[Array[String]] = ArrayBuffer()
      for (elem <- parameters) {
        val pm = elem.split(":")
        if (pm.length != 2) {
          throw new IllegalArgumentException(
            "The" + elem + "format of topic parameters is wrong.For example: keyA:valueA,keyB:valueB,keyC:valueC..."
          )
        }
        paramArray = paramArray :+ pm
      }
      paramArray.map { case Array(x, y) => (x, y) }.toMap
    } else
      Map.empty[String, String]
  }

  private[this] def readProp(path: File): Properties = {
    val properties = new Properties()
    Using(scala.io.Source.fromFile(path)) { bufferedSource =>
      properties.load(bufferedSource.reader())
    }
    properties
  }

  def primaryKeys(
      prop: Map[String, String],
      columnName: Array[String]
  ): Array[String] = {
    val primaryKeys = requireNonidentical(PRIMARY_KEYS, prop)
    val combine = primaryKeys ++ columnName
    if (combine.distinct.length != columnName.length)
      throw new IllegalArgumentException(
        "The " + combine
          .diff(columnName)
          .mkString(
            PRIMARY_KEYS + "(",
            ", ",
            ")"
          ) + " not in column. All " + PRIMARY_KEYS + " should be included in the column."
      )
    primaryKeys
  }

  def requireNonidentical(
      string: String,
      prop: Map[String, String]
  ): Array[String] = {
    val array = prop(string).split(",")
    val distinctArray = array.distinct
    if (array.length != distinctArray.length)
      throw new IllegalArgumentException(
        array
          .diff(distinctArray)
          .mkString(
            string + " (",
            ", ",
            ")"
          ) + " is duplication. The " + string + " should not be duplicated."
      )
    array
  }
}
