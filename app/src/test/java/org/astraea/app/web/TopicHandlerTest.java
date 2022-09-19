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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
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
          Assertions.assertInstanceOf(TopicHandler.Topics.class, handler.get(Channel.EMPTY));
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
          NoSuchElementException.class, () -> handler.get(Channel.ofTarget(Utils.randomString())));
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
              TopicHandler.TopicInfo.class, handler.get(Channel.ofTarget(topicName)));
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

      java.util.function.Consumer<Boolean> test =
          (listInternal) -> {
            Assertions.assertEquals(
                Set.of(topicName), handler.topicNames(Optional.of(topicName), listInternal));
            Assertions.assertThrows(
                NoSuchElementException.class,
                () -> handler.topicNames(Optional.of(Utils.randomString(10)), listInternal));
            Assertions.assertTrue(
                handler.topicNames(Optional.empty(), listInternal).contains(topicName));
          };

      test.accept(true);
      test.accept(false);
    }
  }

  @Test
  void testCreateSingleTopic() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var request =
          Channel.ofRequest(
              PostRequest.of(String.format("{\"topics\":[{\"name\":\"%s\"}]}", topicName)));
      var topics = handler.post(request);
      Assertions.assertEquals(1, topics.topics.size());
      Assertions.assertEquals(topicName, topics.topics.iterator().next().name);
    }
  }

  @Test
  void testCreateTopics() {
    var topicName0 = Utils.randomString(10);
    var topicName1 = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var request =
          Channel.ofRequest(
              PostRequest.of(
                  String.format(
                      "{\"topics\":[{\"name\":\"%s\", \"partitions\":1},{\"partitions\":2,\"name\":\"%s\"}]}",
                      topicName0, topicName1)));
      var topics = handler.post(request);
      Assertions.assertEquals(2, topics.topics.size());
      var t0 = topics.topics.stream().filter(t -> t.name.equals(topicName0)).findFirst().get();
      Assertions.assertEquals(1, t0.partitions.size());
      var t1 = topics.topics.stream().filter(t -> t.name.equals(topicName1)).findFirst().get();
      Assertions.assertEquals(2, t1.partitions.size());
    }
  }

  @Test
  void testDuplicateTopic() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var request =
          Channel.ofRequest(
              PostRequest.of(
                  String.format(
                      "{\"topics\":[{\"name\":\"%s\"},{\"name\":\"%s\"}]}", topicName, topicName)));
      Assertions.assertThrows(IllegalArgumentException.class, () -> handler.post(request));
    }
  }

  @Test
  void testQueryWithPartition() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var request =
          Channel.ofRequest(
              PostRequest.of(
                  String.format("{\"topics\":[{\"name\":\"%s\", \"partitions\":10}]}", topicName)));
      handler.post(request);
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          1,
          Assertions.assertInstanceOf(
                  TopicHandler.TopicInfo.class,
                  handler.get(
                      Channel.ofQueries(topicName, Map.of(TopicHandler.PARTITION_KEY, "0"))))
              .partitions
              .size());

      Assertions.assertEquals(
          10,
          Assertions.assertInstanceOf(
                  TopicHandler.TopicInfo.class, handler.get(Channel.ofTarget(topicName)))
              .partitions
              .size());
    }
  }

  @Test
  void testQueryWithListInternal() {
    var bootstrapServers = bootstrapServers();
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers);
        var producer = Producer.of(bootstrapServers);
        var consumer =
            Consumer.forTopics(Set.of(topicName)).bootstrapServers(bootstrapServers).build()) {
      // producer and consumer here are used to trigger kafka to create internal topic
      // __consumer_offsets
      producer
          .sender()
          .topic(topicName)
          .key("foo".getBytes(UTF_8))
          .run()
          .toCompletableFuture()
          .join();
      consumer.poll(Duration.ofSeconds(1));

      var handler = new TopicHandler(admin);

      var withInternalTopics =
          Assertions.assertInstanceOf(
              TopicHandler.Topics.class,
              handler.get(Channel.ofQueries(Map.of(TopicHandler.LIST_INTERNAL, "true"))));
      Assertions.assertTrue(
          withInternalTopics.topics.stream().anyMatch(t -> t.name.equals("__consumer_offsets")));
      Assertions.assertEquals(
          1, withInternalTopics.topics.stream().filter(t -> t.name.equals(topicName)).count());

      var withoutInternalTopics =
          Assertions.assertInstanceOf(
              TopicHandler.Topics.class,
              handler.get(Channel.ofQueries(Map.of(TopicHandler.LIST_INTERNAL, "false"))));
      Assertions.assertFalse(
          withoutInternalTopics.topics.stream().anyMatch(t -> t.name.equals("__consumer_offsets")));
      Assertions.assertEquals(
          1, withoutInternalTopics.topics.stream().filter(t -> t.name.equals(topicName)).count());
    }
  }

  @Test
  void testCreateTopicWithReplicas() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var request =
          Channel.ofRequest(
              PostRequest.of(
                  String.format(
                      "{\"topics\":[{\"name\":\"%s\",\"partitions\":\"%s\",\"replicas\":\"%s\", \"segment.ms\":\"3000\"}]}",
                      topicName, "2", "2")));
      var topics = handler.post(request);
      Assertions.assertEquals(1, topics.topics.size());
      var topicInfo = topics.topics.iterator().next();
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
      Utils.sleep(Duration.ofSeconds(2));

      handler.delete(Channel.ofTarget(topicNames.get(0)));
      Utils.sleep(Duration.ofSeconds(2));

      var latestTopicNames = admin.topicNames();
      Assertions.assertFalse(latestTopicNames.contains(topicNames.get(0)));
      Assertions.assertTrue(latestTopicNames.contains(topicNames.get(1)));
      Assertions.assertTrue(latestTopicNames.contains(topicNames.get(2)));

      handler.delete(Channel.ofTarget(topicNames.get(2)));
      Utils.sleep(Duration.ofSeconds(2));

      latestTopicNames = admin.topicNames();
      Assertions.assertFalse(latestTopicNames.contains(topicNames.get(2)));
      Assertions.assertTrue(latestTopicNames.contains(topicNames.get(1)));
    }
  }

  @Test
  void testCreateTopicByProbability() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var request =
          Channel.ofRequest(
              PostRequest.of(
                  String.format(
                      "{\"topics\":[{\"name\":\"%s\", \"partitions\":30, \"probability\": 0.5}]}",
                      topicName)));
      var topics = handler.post(request);
      Assertions.assertEquals(1, topics.topics.size());
      Utils.waitFor(
          () ->
              ((TopicHandler.TopicInfo) handler.get(Channel.ofTarget(topicName))).partitions.size()
                  == 30);
      var groupByBroker =
          ((TopicHandler.TopicInfo) handler.get(Channel.ofTarget(topicName)))
              .partitions.stream()
                  .flatMap(p -> p.replicas.stream())
                  .collect(Collectors.groupingBy(r -> r.broker));
      // those brokers should host different number of partitions
      Assertions.assertEquals(
          3, groupByBroker.values().stream().map(List::size).collect(Collectors.toSet()).size());
    }
  }
}
