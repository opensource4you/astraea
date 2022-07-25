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
package org.astraea.app.web;

import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.admin.Admin;
import org.astraea.app.common.Utils;
import org.astraea.app.service.RequireBrokerCluster;
import org.astraea.app.test.CustomAssertions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopicHandlerTest extends RequireBrokerCluster {

  @Test
  void testListTopics() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).create();
      Utils.sleep(Duration.ofSeconds(3));
      var handler = new TopicHandler(admin);
      var response =
          Assertions.assertInstanceOf(
              TopicHandler.Topics.class, handler.get(Optional.empty(), Map.of()));
      Assertions.assertEquals(
          1, response.topics.stream().filter(t -> t.name.equals(topicName)).count());
      Assertions.assertNotEquals(
          0,
          response.topics.stream()
              .filter(t -> t.name.equals(topicName))
              .findFirst()
              .get()
              .configs
              .size());
    }
  }

  @Test
  void testQueryNonexistentTopic() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      Assertions.assertThrows(
          NoSuchElementException.class, () -> handler.get(Optional.of("unknown"), Map.of()));
    }
  }

  @Test
  void testQuerySingleTopic() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).create();
      Utils.sleep(Duration.ofSeconds(3));
      var handler = new TopicHandler(admin);
      var topicInfo =
          Assertions.assertInstanceOf(
              TopicHandler.TopicInfo.class, handler.get(Optional.of(topicName), Map.of()));
      Assertions.assertEquals(topicName, topicInfo.name);
      Assertions.assertNotEquals(0, topicInfo.configs.size());
    }
  }

  @Test
  void testTopics() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).create();
      Utils.sleep(Duration.ofSeconds(3));
      var handler = new TopicHandler(admin);
      Assertions.assertEquals(Set.of(topicName), handler.topicNames(Optional.of(topicName)));
      Assertions.assertThrows(
          NoSuchElementException.class,
          () -> handler.topicNames(Optional.of(Utils.randomString(10))));
      Assertions.assertTrue(handler.topicNames(Optional.empty()).contains(topicName));
    }
  }

  @Test
  void testCreateTopic() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var topicInfo =
          Assertions.assertInstanceOf(
              TopicHandler.TopicInfo.class,
              handler.post(PostRequest.of(Map.of(TopicHandler.TOPIC_NAME_KEY, topicName))));
      Assertions.assertEquals(topicName, topicInfo.name);
    }
  }

  @Test
  void testQueryWithPartition() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      Assertions.assertInstanceOf(
          TopicHandler.TopicInfo.class,
          handler.post(
              PostRequest.of(
                  Map.of(
                      TopicHandler.TOPIC_NAME_KEY,
                      topicName,
                      TopicHandler.NUMBER_OF_PARTITIONS_KEY,
                      "10"))));
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          1,
          Assertions.assertInstanceOf(
                  TopicHandler.TopicInfo.class,
                  handler.get(Optional.of(topicName), Map.of(TopicHandler.PARTITION_KEY, "0")))
              .partitions
              .size());

      Assertions.assertEquals(
          10,
          Assertions.assertInstanceOf(
                  TopicHandler.TopicInfo.class, handler.get(Optional.of(topicName), Map.of()))
              .partitions
              .size());
    }
  }

  @Test
  void testCreateTopicWithReplicas() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var topicInfo =
          Assertions.assertInstanceOf(
              TopicHandler.TopicInfo.class,
              handler.post(
                  PostRequest.of(
                      Map.of(
                          TopicHandler.TOPIC_NAME_KEY,
                          topicName,
                          TopicHandler.NUMBER_OF_PARTITIONS_KEY,
                          "2",
                          TopicHandler.NUMBER_OF_REPLICAS_KEY,
                          "2",
                          "segment.ms",
                          "3000"))));
      Assertions.assertEquals(topicName, topicInfo.name);

      // the topic creation is not synced, so we have to wait the creation.
      if (topicInfo.partitions.isEmpty()) {
        Utils.sleep(Duration.ofSeconds(2));
        var result = admin.replicas(Set.of(topicName));
        Assertions.assertEquals(2, result.size());
        result.values().forEach(replicas -> Assertions.assertEquals(2, replicas.size()));
      } else {
        Assertions.assertEquals(2, topicInfo.partitions.size());
        Assertions.assertEquals(2, topicInfo.partitions.iterator().next().replicas.size());
      }
      Assertions.assertEquals(
          "3000", admin.topics(Set.of(topicName)).get(topicName).value("segment.ms").get());
    }
  }

  @Test
  void testRemainingConfigs() {
    Assertions.assertEquals(
        0,
        TopicHandler.remainingConfigs(
                PostRequest.of(
                    Map.of(
                        TopicHandler.TOPIC_NAME_KEY,
                        "abc",
                        TopicHandler.NUMBER_OF_PARTITIONS_KEY,
                        "2",
                        TopicHandler.NUMBER_OF_REPLICAS_KEY,
                        "2")))
            .size());

    Assertions.assertEquals(
        1,
        TopicHandler.remainingConfigs(
                PostRequest.of(Map.of(TopicHandler.TOPIC_NAME_KEY, "abc", "key", "value")))
            .size());
  }

  @Test
  void testDeleteTopic() {
    var topicNames =
        IntStream.range(0, 3).mapToObj(x -> Utils.randomString(10)).collect(Collectors.toList());
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      topicNames.forEach(
          x -> admin.creator().topic(x).numberOfPartitions(3).numberOfReplicas((short) 3).create());

      handler.delete(topicNames.get(0), Map.of());
      CustomAssertions.assertContain(
          Set.of(topicNames.get(1), topicNames.get(2)), admin.topicNames());

      handler.delete(topicNames.get(2), Map.of());
      CustomAssertions.assertContain(Set.of(topicNames.get(1)), admin.topicNames());
    }
  }
}
