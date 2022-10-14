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

import org.astraea.common.admin.AsyncAdmin
import org.astraea.it.RequireBrokerCluster
import org.astraea.it.RequireBrokerCluster.bootstrapServers
import org.junit.jupiter.api.Assertions.{
  assertEquals,
  assertInstanceOf,
  assertThrows
}
import org.junit.jupiter.api.Test

import java.io.File
import java.util.concurrent.CompletionException
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class KafkaWriterTest extends RequireBrokerCluster {

  @Test def TopicCreatorTest(): Unit = {
    val TOPIC = "test-topicA"
    Utils.Using(AsyncAdmin.of(bootstrapServers)) { admin =>
      {
        Await.result(testTopicCreator(admin, TOPIC), Duration.Inf)
        assert(
          admin.topicNames(true).toCompletableFuture.get().contains(TOPIC),
          true
        )
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
          "gzip"
        )

        testTopicCreator(admin, TOPIC)
      }
    }
  }

  @Test def IllegalArgumentTopicCreatorTest(): Unit = {
    val TOPIC = "test-topicB"

    Utils.Using(AsyncAdmin.of(bootstrapServers)) { admin =>
      {
        val partition = Metadata(
          new File(""),
          new File(""),
          Map.empty,
          Map.empty,
          bootstrapServers(),
          TOPIC,
          2,
          2,
          Map("compression.type" -> "gzip"),
          "local[2]"
        )
        Await.result(testTopicCreator(admin, TOPIC), Duration.Inf)
        assertInstanceOf(
          classOf[IllegalArgumentException],
          assertThrows(
            classOf[CompletionException],
            () =>
              Await.result(
                KafkaWriter.createTopic(admin, partition),
                Duration.Inf
              )
          ).getCause
        )

        val replica = Metadata(
          new File(""),
          new File(""),
          Map.empty,
          Map.empty,
          bootstrapServers(),
          TOPIC,
          10,
          1,
          Map("compression.type" -> "gzip"),
          "local[2]"
        )

        assertInstanceOf(
          classOf[IllegalArgumentException],
          assertThrows(
            classOf[CompletionException],
            () =>
              Await.result(
                KafkaWriter.createTopic(admin, replica),
                Duration.Inf
              )
          ).getCause
        )

        val config = Metadata(
          new File(""),
          new File(""),
          Map.empty,
          Map.empty,
          bootstrapServers(),
          TOPIC,
          10,
          1,
          Map("compression.type" -> "lz4"),
          "local[2]"
        )

        assertInstanceOf(
          classOf[IllegalArgumentException],
          assertThrows(
            classOf[CompletionException],
            () =>
              Await.result(
                KafkaWriter.createTopic(admin, config),
                Duration.Inf
              )
          ).getCause
        )
      }
    }
  }

  def testTopicCreator(
      asyncAdmin: AsyncAdmin,
      TOPIC: String
  ): Future[java.lang.Boolean] = {
    val config = Map("compression.type" -> "gzip")
    val metadata = Metadata(
      new File(""),
      new File(""),
      Map.empty,
      Map.empty,
      bootstrapServers(),
      TOPIC,
      10,
      2,
      config,
      "local[2]"
    )
    KafkaWriter.createTopic(asyncAdmin, metadata)
  }
}
