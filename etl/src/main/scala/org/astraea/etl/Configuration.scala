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
import java.util.{Calendar, Properties}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Random, Using}

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
  * @param topicParameters
  *   The rest of the topic can be configured parameters.For example:
  *   keyA:valueA,keyB:valueB,keyC:valueC...
  */
case class Configuration(
    sourcePath: File,
    sinkPath: File,
    columnName: Array[String],
    primaryKeys: String,
    kafkaBootstrapServers: String,
    topicName: String,
    numPartitions: Int,
    numReplicas: Int,
    topicParameters: Map[String, String]
)

object Configuration {
  val default_num_partitions = 15;
  val default_num_replicas = 3;

  //Parameters needed to configure ETL.
  def apply(path: String): Configuration = {
    val properties = readProp(path).asScala.filter(_._2.nonEmpty).toMap
    Configuration(
      Utils.requireFolder(properties.getOrElse("source.path", "")),
      Utils.requireFolder(properties.getOrElse("sink.path", "")),
      //TODO check the format after reading CSV
      properties("column.name").split(","),
      properties("primary.keys"),
      //TODO check the format after linking Kafka
      properties("kafka.bootstrap.servers"),
      properties.getOrElse(
        "topic.name",
        "spark-" + Calendar
          .getInstance()
          .get(Calendar.MILLISECOND) + "-" + Random.nextInt(1000)
      ),
      properties
        .get("topic.num.partitions")
        .map(_.toInt)
        .getOrElse(default_num_partitions),
      properties
        .get("topic.num.replicas")
        .map(_.toInt)
        .getOrElse(default_num_replicas),
      topicParameters(properties.getOrElse("topic.parameters", ""))
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
            "The format of topic parameters is wrong.For example: keyA:valueA,keyB:valueB,keyC:valueC..."
          )
        }
        paramArray = paramArray :+ pm
      }
      paramArray.map { case Array(x, y) => (x, y) }.toMap
    } else
      Map.empty[String, String]
  }

  def readProp(path: String): Properties = {
    val properties = new Properties()
    Using(scala.io.Source.fromFile(path)) { bufferedSource =>
      properties.load(bufferedSource.reader())
    }
    properties
  }
}
