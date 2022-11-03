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
import org.astraea.common.admin.Admin
import org.astraea.common.consumer.{Consumer, Deserializer}
import org.astraea.etl.FileCreator.{generateCSVF, mkdir}
import org.astraea.etl.Spark2KafkaTest.{COL_NAMES, rows, sinkD, source}
import org.astraea.it.RequireBrokerCluster
import org.astraea.it.RequireBrokerCluster.bootstrapServers
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeAll, Disabled, Test}

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.{
  `collection AsScalaIterable`,
  `collection asJava`
}
import scala.concurrent.duration.Duration
import scala.util.Random

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

    rowData.forEach(record => assertEquals(records(record._1), record._2))
  }

  @Test
  def topicCheckTest(): Unit = {
    val TOPIC = "testTopic"
    Utils.Using(Admin.of(bootstrapServers())) { admin =>
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

  @Test def archiveTest(): Unit = {
    Thread.sleep(Duration(20, TimeUnit.SECONDS).toMillis)
    assertTrue(
      Files.exists(
        new File(
          sinkD + source + "/local_kafka-" + "0" + ".csv"
        ).toPath
      )
    )
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
            ).head}","${colNames(2)}":"${rows(i)(1)}","${colNames(3)}":${rows(
              i
            )(
              2
            )}}"""
        )
      )
      .toMap
  }
}

object Spark2KafkaTest extends RequireBrokerCluster {
  private val tempPath: String =
    System.getProperty("java.io.tmpdir") + "/sparkFile" + Random.nextInt()
  private val source: String = tempPath + "/source"
  private val sinkD: String = tempPath + "/sink"
  private val COL_NAMES =
    "ID=integer,FirstName=string,SecondName=string,Age=integer"

  @BeforeAll
  def setup(): Unit = {
    val myDir = mkdir(tempPath)
    val sourceDir = mkdir(tempPath + "/source")
    val sinkDir = mkdir(sinkD)
    val checkoutDir = mkdir(tempPath + "/checkout")
    val dataDir = mkdir(tempPath + "/data")
    val myPropDir =
      Files.createFile(new File(myDir + "/prop.properties").toPath)
    generateCSVF(sourceDir, rows)

    writeProperties(myPropDir.toFile, sourceDir.getPath, sinkDir.getPath)
    Spark2Kafka.executor(
      Array(myPropDir.toString),
      Duration(20, TimeUnit.SECONDS)
    )
  }

  private def writeProperties(
      file: File,
      sourcePath: String,
      sinkPath: String
  ): Unit = {
    val SOURCE_PATH = "source.path"
    val SINK_PATH = "sink.path"
    val COLUMN_NAME = "column.name"
    val PRIMARY_KEYS = "primary.keys"
    val KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"
    val TOPIC_NAME = "topic.name"
    val TOPIC_PARTITIONS = "topic.partitions"
    val TOPIC_REPLICAS = "topic.replicas"
    val TOPIC_CONFIG = "topic.config"
    val DEPLOY_MODEL = "deploy.model"

    Utils.Using(new FileOutputStream(file)) { fileOut =>
      val properties = new Properties()
      properties.setProperty(SOURCE_PATH, sourcePath)
      properties.setProperty(SINK_PATH, sinkPath)
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
      properties.setProperty(DEPLOYMENT_MODEL, "local[1]")

      properties.store(fileOut, "Favorite Things");
    }
  }

  private def rows: List[List[String]] = {
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
}
