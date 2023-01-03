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

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.astraea.common.consumer.{Consumer, Deserializer}
import org.astraea.etl.FileCreator.generateCSVF
import org.astraea.etl.Spark2KafkaTest.{COL_NAMES, rows}
import org.astraea.it.RequireBrokerCluster
import org.astraea.it.RequireBrokerCluster.bootstrapServers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeAll, Test}

import java.nio.file.Files
import java.util
import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

class Spark2KafkaTest extends RequireBrokerCluster {
  @Test
  def consumerDataTest(): Unit = {
    val topic = new util.HashSet[String]
    topic.add("testTopic")

    val consumer =
      Consumer
        .forTopics(topic)
        .bootstrapServers(bootstrapServers())
        .keyDeserializer(Deserializer.STRING)
        .valueDeserializer(Deserializer.STRING)
        .configs(
          Map(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest").asJava
        )
        .build()

    val records =
      Range
        .inclusive(0, 5)
        .flatMap(_ => consumer.poll(java.time.Duration.ofSeconds(1)).asScala)
        .map(record => (record.key(), record.value()))
        .toMap

    val rowData = s2kType(rows)

    records.foreach(records => assertEquals(records._2, rowData(records._1)))
  }

  def s2kType(rows: List[List[String]]): Map[String, String] = {
    val colNames =
      COL_NAMES.split(",").map(_.split("=")).map(elem => elem(0)).toSeq
    Range
      .inclusive(0, 3)
      .map(i =>
        (
          s"""{"${colNames.head}":"${rows(
              i
            ).head}","${colNames(1)}":"${rows(i)(1)}"}""",
          s"""{"${colNames(2)}":"${rows(
              i
            )(
              2
            )}","${colNames.head}":"${rows(
              i
            ).head}","${colNames(1)}":"${rows(i)(1)}"}"""
        )
      )
      .toMap
  }
}

object Spark2KafkaTest extends RequireBrokerCluster {
  private val COL_NAMES =
    "FirstName=string,SecondName=string,Age=integer"

  @BeforeAll
  def setup(): Unit = {
    val sourceDir = Files.createTempDirectory("source").toFile
    val checkoutDir = Files.createTempDirectory("checkpoint").toFile
    generateCSVF(sourceDir, rows)

    val metadata = Metadata(
      sourcePath = sourceDir.getPath,
      checkpoint = checkoutDir.getPath,
      columns = immutable.Seq(
        DataColumn(
          name = "FirstName",
          isPk = true,
          dataType = DataType.StringType
        ),
        DataColumn(
          name = "SecondName",
          isPk = true,
          dataType = DataType.StringType
        ),
        DataColumn(name = "Age", dataType = DataType.StringType)
      ),
      kafkaBootstrapServers = bootstrapServers(),
      topicName = "testTopic",
      topicConfigs = Map("compression.type" -> "lz4"),
      numberOfPartitions = 10,
      numberOfReplicas = 1
    )

    Spark2Kafka.executor(
      SparkSession
        .builder()
        .master("local[2]")
        .getOrCreate(),
      metadata,
      Duration("10 seconds")
    )
  }

  private def rows: List[List[String]] = {
    val columnOne: List[String] =
      List("Michael", "Andy", "Justin", "")
    val columnTwo: List[String] =
      List("A.K", "B.C", "C.L", "")
    val columnThree: List[String] =
      List("29", "30", "19", "")

    columnOne
      .zip(columnTwo.zip(columnThree))
      .foldLeft(List.empty[List[String]]) { case (acc, (a, (b, c))) =>
        List(a, b, c) +: acc
      }
      .reverse
  }
}
