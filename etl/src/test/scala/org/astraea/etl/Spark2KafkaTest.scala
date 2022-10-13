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
import org.astraea.common.admin.AsyncAdmin
import org.astraea.common.consumer.{Consumer, Deserializer}
import org.astraea.etl.FileCreator.{addPrefix, writeCsvFile}
import org.astraea.etl.Spark2KafkaTest.hasPerform
import org.astraea.it.RequireBrokerCluster
import org.astraea.it.RequireBrokerCluster.bootstrapServers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.util
import java.util.Properties
import scala.util.Random
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.{
  `collection AsScalaIterable`,
  `collection asJava`
}
import scala.concurrent.duration.DurationInt

class Spark2KafkaTest extends RequireBrokerCluster {
  private val COL_NAMES =
    "ID=integer,FirstName=string,SecondName=string,Age=integer"
  setup()

  def setup(): Unit = {
    hasPerform.synchronized {
      if (!hasPerform) {
        hasPerform = true
        val tempPath = System.getProperty("java.io.tmpdir")
        val myCSVDir = new File(tempPath + "/spark-" + Random.nextInt())
        myCSVDir.mkdir()
        val file = Files.createTempFile(myCSVDir.toPath, "local_kafka", ".csv")
        writeCsvFile(file.toAbsolutePath.toString, addPrefix(rows))

        val myPropDir = new File(tempPath + "/prop-" + Random.nextInt())
        myPropDir.mkdir()
        val properties = Files.createTempFile(
          myPropDir.toPath,
          "spark2kafkaConfig",
          ".properties"
        )
        writeProperties(properties.toFile, file.getParent.toString)
        Spark2Kafka.executor(
          Array(properties.toString),
          new DurationInt(10).second
        )
      }
    }
  }

  @Test
  def consumerDataTest(): Unit = {
    val topic = new util.HashSet[String]
    topic.add("testTopic")
    println(
      AsyncAdmin
        .of(bootstrapServers())
        .topicNames(true)
        .toCompletableFuture
        .get()
    )
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

    val rowData = s2kType(rows)

    records.forEach(record =>
      assertEquals(rowData(record.key()), record.value())
    )
  }

  @Test
  def topicCheckTest(): Unit = {
    val TOPIC = "testTopic"
    Utils.Using(AsyncAdmin.of(bootstrapServers())) { admin =>
      assertEquals(
        admin
          .partitions(Set(TOPIC).asJava)
          .toCompletableFuture
          .get()
          .size(),
        10
      )

      admin
        .partitions(Set(TOPIC).asJava)
        .toCompletableFuture
        .get()
        .forEach(partition => assertEquals(partition.replicas().size(), 2))

      assertEquals(
        admin
          .topics(Set(TOPIC).asJava)
          .toCompletableFuture
          .get()
          .head
          .config()
          .raw()
          .get("compression.type"),
        "lz4"
      )
    }
  }

  def s2kType(rows: List[List[String]]): Map[String, String] = {
    val colNames =
      COL_NAMES.split(",").map(_.split("=")).map(elem => elem(0)).toSeq
    Range
      .inclusive(0, 3)
      .map(i =>
        (
          s"${rows(i).head},${rows(i)(1)}",
          s"""{"${colNames.head}":${i + 1},"${colNames(1)}":"${rows(
            i
          ).head}","${colNames(2)}":"${rows(i)(1)}","${colNames(3)}":${rows(i)(
            2
          )}}"""
        )
      )
      .toMap
  }

  def rows: List[List[String]] = {
    val columnOne: List[String] =
      List("Michael", "Andy", "Justin", "LuLu")
    val columnTwo: List[String] =
      List("A.K", "B.C", "C.L", "C.C")
    val columnThree: List[String] =
      List("29", "30", "19", "18")

    columnOne
      .zip(columnTwo.zip(columnThree))
      .foldLeft(List.empty[List[String]]) { case (acc, (a, (b, c))) =>
        List(a, b, c) +: acc
      }
      .reverse
  }

  def writeProperties(file: File, source_path: String): Unit = {
    val SOURCE_PATH = "source.path"
    val SINK_PATH = "sink.path"
    val COLUMN_NAME = "column.name"
    val PRIMARY_KEYS = "primary.keys"
    val KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"
    val TOPIC_NAME = "topic.name"
    val TOPIC_PARTITIONS = "topic.partitions"
    val TOPIC_REPLICAS = "topic.replicas"
    val TOPIC_CONFIG = "topic.config"
    val DEPLOYMENT_MODEL = "deployment.model"

    Utils.Using(new FileOutputStream(file)) { fileOut =>
      val properties = new Properties();
      properties.setProperty(SOURCE_PATH, source_path)
      properties.setProperty(SINK_PATH, source_path)
      properties.setProperty(
        COLUMN_NAME,
        "ID=integer,FirstName=string,SecondName=string,Age=integer"
      )
      properties.setProperty(PRIMARY_KEYS, "FirstName=string,SecondName=string")
      properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, bootstrapServers())
      properties.setProperty(TOPIC_NAME, "testTopic")
      properties.setProperty(TOPIC_PARTITIONS, "10")
      properties.setProperty(TOPIC_REPLICAS, "2")
      properties.setProperty(TOPIC_CONFIG, "compression.type=lz4")
      properties.setProperty(DEPLOYMENT_MODEL, "local[2]")

      properties.store(fileOut, "Favorite Things");
    }
  }
}

object Spark2KafkaTest {
  var hasPerform = false
}
