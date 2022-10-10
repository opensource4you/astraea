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
package org.astraea.etl.KafkaAdmin

import org.astraea.common.admin.AsyncAdmin
import org.astraea.etl.Utils
import org.astraea.it.RequireBrokerCluster
import org.astraea.it.RequireBrokerCluster.bootstrapServers
import org.junit.jupiter.api.Assertions.{assertInstanceOf, assertThrows}
import org.junit.jupiter.api.Test

import java.util.concurrent.CompletionException
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TopicCreatorTest extends RequireBrokerCluster {

  @Test def TopicCreatorTest(): Unit = {
    val TOPIC = "test-topicA"
    Utils.Using(AsyncAdmin.of(bootstrapServers)) { admin =>
      {
        Await.result(testTopicCreator(admin, TOPIC), Duration.Inf)

        println(admin.topicNames(true).toCompletableFuture.get().size())
        assert(
          admin.topicNames(true).toCompletableFuture.get().contains(TOPIC),
          true
        )
        assert(
          admin
            .partitions(Set(TOPIC).asJava)
            .toCompletableFuture
            .get()
            .size() equals 10
        )
        admin
          .partitions(Set(TOPIC).asJava)
          .toCompletableFuture
          .get()
          .forEach(partition => assert(partition.replicas().size() equals 2))
        assert(
          admin
            .topics(Set(TOPIC).asJava)
            .toCompletableFuture
            .get()
            .head
            .config()
            .raw()
            .get("compression.type") equals "gzip"
        )

        testTopicCreator(admin, TOPIC)
      }
    }
  }

  @Test def IllegalArgumentTopicCreatorTest(): Unit = {
    val TOPIC = "test-topicB"

    Utils.Using(AsyncAdmin.of(bootstrapServers)) { admin =>
      {
        Await.result(testTopicCreator(admin, TOPIC), Duration.Inf)

        assertInstanceOf(
          classOf[IllegalArgumentException],
          assertThrows(
            classOf[CompletionException],
            () =>
              Await.result(
                TopicCreatorImpl
                  .apply(admin)
                  .topic(TOPIC)
                  .numberOfPartitions(2)
                  .numberOfReplicas(2)
                  .create(),
                Duration.Inf
              )
          ).getCause
        )

        assertInstanceOf(
          classOf[IllegalArgumentException],
          assertThrows(
            classOf[CompletionException],
            () =>
              Await.result(
                TopicCreatorImpl
                  .apply(admin)
                  .topic(TOPIC)
                  .numberOfPartitions(10)
                  .numberOfReplicas(1)
                  .create(),
                Duration.Inf
              )
          ).getCause
        )

        assertInstanceOf(
          classOf[IllegalArgumentException],
          assertThrows(
            classOf[CompletionException],
            () =>
              Await.result(
                TopicCreatorImpl
                  .apply(admin)
                  .topic(TOPIC)
                  .numberOfPartitions(10)
                  .numberOfReplicas(2)
                  .config(Map("compression.type" -> "lz4"))
                  .create(),
                Duration.Inf
              )
          ).getCause
        )
      }
    }
  }

  def testTopicCreator(
      admin: AsyncAdmin,
      TOPIC: String
  ): Future[java.lang.Boolean] = {
    val config = Map("compression.type" -> "gzip")
    TopicCreatorImpl
      .apply(admin)
      .topic(TOPIC)
      .numberOfPartitions(10)
      .numberOfReplicas(2)
      .config(config)
      .create()
  }
}
