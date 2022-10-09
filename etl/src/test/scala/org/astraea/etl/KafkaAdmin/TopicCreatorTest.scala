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

import org.astraea.common.admin.{AsyncAdmin}
import org.astraea.etl.Utils
import org.astraea.it.RequireBrokerCluster
import org.astraea.it.RequireBrokerCluster.bootstrapServers
import org.junit.jupiter.api.Assertions.{assertInstanceOf, assertThrows}
import org.junit.jupiter.api.Test

import java.util.concurrent.ExecutionException
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class TopicCreatorTest extends RequireBrokerCluster {

  @Test def TopicCreatorTest(): Unit = {
    val TOPIC = "test-topicA"
    Utils.Using(AsyncAdmin.of(bootstrapServers)) { admin =>
      {
        testTopicCreator(admin, TOPIC)
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
        testTopicCreator(admin, TOPIC)

        assertInstanceOf(
          classOf[IllegalArgumentException],
          assertThrows(
            classOf[ExecutionException],
            () =>
              TopicCreatorImpl
                .apply(admin)
                .topic(TOPIC)
                .numberOfPartitions(2)
                .numberOfReplicas(2)
                .create()
                .get
          ).getCause
        )

        assertInstanceOf(
          classOf[IllegalArgumentException],
          assertThrows(
            classOf[ExecutionException],
            () =>
              TopicCreatorImpl
                .apply(admin)
                .topic(TOPIC)
                .numberOfPartitions(10)
                .numberOfReplicas(1)
                .create()
                .get()
          ).getCause
        )

        assertInstanceOf(
          classOf[IllegalArgumentException],
          assertThrows(
            classOf[ExecutionException],
            () =>
              TopicCreatorImpl
                .apply(admin)
                .topic(TOPIC)
                .numberOfPartitions(10)
                .numberOfReplicas(2)
                .config(Map("compression.type" -> "lz4"))
                .create()
                .get()
          ).getCause
        )
      }
    }
  }

  def testTopicCreator(admin: AsyncAdmin, TOPIC: String): Unit = {
    val config = Map("compression.type" -> "gzip")
    TopicCreatorImpl
      .apply(admin)
      .topic(TOPIC)
      .numberOfPartitions(10)
      .numberOfReplicas(2)
      .config(config)
      .create()
      .get()
  }
}
