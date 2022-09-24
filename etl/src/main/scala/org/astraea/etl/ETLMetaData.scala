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
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Parameters required for Astraea ETL.
  *
  * @param sourcePath
  *   The data source path should be a directory.
  * @param sinkPath
  *   The data sink path should be a directory.
  * @param column
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
  * @param deploymentModel
  *   Set deployment model, which will be used in
  *   SparkSession.builder().master(deployment.model).Two settings are currently
  *   supported spark://HOST:PORT and local[*].
  */
case class ETLMetaData(
    sourcePath: File,
    sinkPath: File,
    column: Map[String, String],
    primaryKeys: Map[String, String],
    kafkaBootstrapServers: String,
    topicName: String,
    numPartitions: Int,
    numReplicas: Int,
    topicConfig: Map[String, String],
    deploymentModel: String
)

object ETLMetaData {
  private[this] val SOURCE_PATH = "source.path"
  private[this] val SINK_PATH = "sink.path"
  private[this] val COLUMN_NAME = "column.name"
  private[this] val PRIMARY_KEYS = "primary.keys"
  private[this] val KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"
  private[this] val TOPIC_NAME = "topic.name"
  private[this] val TOPIC_PARTITIONS = "topic.partitions"
  private[this] val TOPIC_REPLICAS = "topic.replicas"
  private[this] val TOPIC_CONFIG = "topic.config"
  private[this] val DEPLOYMENT_MODEL = "deployment.model"

  private[this] val DEFAULT_PARTITIONS = 15
  private[this] val DEFAULT_REPLICAS = 1

  //Parameters needed to configure ETL.
  def apply(path: File): ETLMetaData = {
    val properties = readProp(path).asScala.filter(_._2.nonEmpty).toMap

    val sourcePath = Utils.requireFolder(
      properties.getOrElse(
        SOURCE_PATH,
        throw new NullPointerException(
          s"$SOURCE_PATH is null. You must configure $SOURCE_PATH."
        )
      )
    )
    val sinkPath = Utils.requireFolder(
      properties.getOrElse(
        SINK_PATH,
        throw new NullPointerException(
          s"$SINK_PATH + is null.You must configure $SINK_PATH."
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
        s"$TOPIC_NAME is null.You must configure $TOPIC_NAME."
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
    val topicConfig = requirePair(properties.getOrElse(TOPIC_CONFIG, null))

    val deploymentModel = requireSparkMaster(DEPLOYMENT_MODEL, properties)

    ETLMetaData(
      sourcePath,
      sinkPath,
      column,
      pKeys,
      bootstrapServer,
      topicName,
      topicPartitions,
      topicReplicas,
      topicConfig,
      deploymentModel
    )
  }

  //Handling the topic.parameters parameter.
  def requirePair(tConfig: String): Map[String, String] = {
    Option(tConfig)
      .map(
        _.split(",")
          .map(_.split("="))
          .map { elem =>
            if (elem.length != 2) {
              throw new IllegalArgumentException(
                s"The ${elem.mkString(",")} format of topic parameters is wrong.For example: keyA=valueA,keyB=valueB,keyC=valueC..."
              )
            }
            (elem(0), elem(1))
          }
          .toMap
      )
      .getOrElse(Map.empty[String, String])
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
      columnName: Map[String, String]
  ): Map[String, String] = {
    val primaryKeys = requireNonidentical(PRIMARY_KEYS, prop)
    val combine = primaryKeys.keys.toArray ++ columnName.keys.toArray
    if (combine.distinct.length != columnName.size) {
      val column = columnName.keys.toArray
      throw new IllegalArgumentException(
        s"The ${combine
          .diff(column)
          .mkString(
            PRIMARY_KEYS + "(",
            ", ",
            ")"
          )} not in column. All $PRIMARY_KEYS should be included in the column."
      )
    }
    primaryKeys
  }

  def requireNonidentical(
      string: String,
      prop: Map[String, String]
  ): Map[String, String] = {
    val array = prop(string).split(",")
    val map = requirePair(prop(string))
    if (map.size != array.length) {
      val column = map.keys.toArray
      throw new IllegalArgumentException(
        s"${array
          .diff(column)
          .mkString(
            string + " (",
            ", ",
            ")"
          )} is duplication. The $string should not be duplicated."
      )
    }
    map
  }

  def requireSparkMaster(string: String, prop: Map[String, String]): String = {
    if (
      !(Utils
        .localPattern(prop(string)) || Utils.standAlonePattern(prop(string)))
    ) {
      throw new IllegalArgumentException(
        s"${prop { string }} not a supported deployment model. Please check $string."
      )
    }
    string
  }
}
