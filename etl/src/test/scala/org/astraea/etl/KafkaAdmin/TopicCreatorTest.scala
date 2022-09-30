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

import org.astraea.common.admin.Admin
import org.astraea.etl.Utils
import org.astraea.it.RequireBrokerCluster
import org.astraea.it.RequireBrokerCluster.bootstrapServers
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class TopicCreatorTest extends RequireBrokerCluster {
  @Test def TopicCreatorTest(): Unit = {
    val TOPIC = "test-topic"

    Utils.withResources(Admin.of(bootstrapServers)) { admin =>
      {
        testTopicCreator(admin)

        assert(admin.topicNames().contains(TOPIC), true)
        assert(admin.partitions(Set(TOPIC).asJava).size() equals 10)
        admin
          .replicas(Set(TOPIC).asJava)
          .values()
          .forEach(replicas => assert(replicas.size() equals 2))
        assert(
          admin
            .topics(Set(TOPIC).asJava)
            .head
            .config()
            .raw()
            .get("compression.type") equals "gzip"
        )

        testTopicCreator(admin)
      }
    }
  }

  @Test def IllegalArgumentTopicCreatorTest(): Unit = {
    val TOPIC = "test-topic"

    Utils.withResources(Admin.of(bootstrapServers)) { admin =>
      {
        testTopicCreator(admin)

        assertThrows(
          classOf[IllegalArgumentException],
          () =>
            TopicCreatorImpl
              .apply(admin)
              .topic(TOPIC)
              .numberOfPartitions(2)
              .numberOfReplicas(2)
              .create()
        )

        assertThrows(
          classOf[IllegalArgumentException],
          () =>
            TopicCreatorImpl
              .apply(admin)
              .topic(TOPIC)
              .numberOfPartitions(10)
              .numberOfReplicas(1)
              .create()
        )

        assertThrows(
          classOf[IllegalArgumentException],
          () =>
            TopicCreatorImpl
              .apply(admin)
              .topic(TOPIC)
              .numberOfPartitions(10)
              .numberOfReplicas(2)
              .config(Map("compression.type" -> "lz4"))
              .create()
        )
      }
    }
  }

  def testTopicCreator(admin: Admin): Unit = {
    val config = Map("compression.type" -> "gzip")
    val TOPIC = "test-topic"

    TopicCreatorImpl
      .apply(admin)
      .topic(TOPIC)
      .numberOfPartitions(10)
      .numberOfReplicas(2)
      .config(config)
      .create()
  }
}
