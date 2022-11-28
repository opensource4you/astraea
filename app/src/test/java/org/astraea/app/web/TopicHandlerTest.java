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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopicHandlerTest extends RequireBrokerCluster {

  @Test
  void testListTopics() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));
      var handler = new TopicHandler(admin);
      var response =
          Assertions.assertInstanceOf(
              TopicHandler.Topics.class, handler.get(Channel.EMPTY).toCompletableFuture().join());
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
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      Assertions.assertInstanceOf(
          NoSuchElementException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      handler
                          .get(Channel.ofTarget(Utils.randomString()))
                          .toCompletableFuture()
                          .join())
              .getCause());
    }
  }

  @Test
  void testQuerySingleTopic() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));
      var handler = new TopicHandler(admin);
      var topicInfo =
          Assertions.assertInstanceOf(
              TopicHandler.TopicInfo.class,
              handler.get(Channel.ofTarget(topicName)).toCompletableFuture().join());
      Assertions.assertEquals(topicName, topicInfo.name);
      Assertions.assertNotEquals(0, topicInfo.configs.size());
    }
  }

  @Test
  void testCreateSingleTopic() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var request = Channel.ofRequest(String.format("{\"topics\":[{\"name\":\"%s\"}]}", topicName));
      var topics = handler.post(request).toCompletableFuture().join();
      Assertions.assertEquals(1, topics.topics.size());
      Assertions.assertEquals(topicName, topics.topics.iterator().next().name);
    }
  }

  @Test
  void testCreateTopics() {
    var topicName0 = Utils.randomString(10);
    var topicName1 = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var request =
          Channel.ofRequest(
              String.format(
                  "{\"topics\":[{\"name\":\"%s\", \"partitions\":1},{\"partitions\":2,\"name\":\"%s\"}]}",
                  topicName0, topicName1));
      var topics = handler.post(request).toCompletableFuture().join();
      Assertions.assertEquals(2, topics.topics.size());
      // the topic creation is not synced, so we have to wait the creation.
      Utils.sleep(Duration.ofSeconds(2));

      var actualTopPartitions =
          admin.topicPartitions(Set.of(topicName0, topicName1)).toCompletableFuture().join();
      Assertions.assertEquals(
          1, actualTopPartitions.stream().filter(tp -> tp.topic().equals(topicName0)).count());
      Assertions.assertEquals(
          2, actualTopPartitions.stream().filter(tp -> tp.topic().equals(topicName1)).count());
    }
  }

  @Test
  void testDuplicateTopic() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var request =
          Channel.ofRequest(
              String.format(
                  "{\"topics\":[{\"name\":\"%s\"},{\"name\":\"%s\"}]}", topicName, topicName));
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> handler.post(request).toCompletableFuture().join());
    }
  }

  @Test
  void testQueryWithPartition() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var request =
          Channel.ofRequest(
              String.format("{\"topics\":[{\"name\":\"%s\", \"partitions\":10}]}", topicName));
      handler.post(request).toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          1,
          Assertions.assertInstanceOf(
                  TopicHandler.TopicInfo.class,
                  handler
                      .get(Channel.ofQueries(topicName, Map.of(TopicHandler.PARTITION_KEY, "0")))
                      .toCompletableFuture()
                      .join())
              .partitions
              .size());

      Assertions.assertEquals(
          10,
          Assertions.assertInstanceOf(
                  TopicHandler.TopicInfo.class,
                  handler.get(Channel.ofTarget(topicName)).toCompletableFuture().join())
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
          .send(Record.builder().topic(topicName).key("foo".getBytes(UTF_8)).build())
          .toCompletableFuture()
          .join();
      consumer.poll(Duration.ofSeconds(1));

      var handler = new TopicHandler(admin);

      var withInternalTopics =
          Assertions.assertInstanceOf(
              TopicHandler.Topics.class,
              handler
                  .get(Channel.ofQueries(Map.of(TopicHandler.LIST_INTERNAL, "true")))
                  .toCompletableFuture()
                  .join());
      Assertions.assertTrue(
          withInternalTopics.topics.stream().anyMatch(t -> t.name.equals("__consumer_offsets")));
      Assertions.assertEquals(
          1, withInternalTopics.topics.stream().filter(t -> t.name.equals(topicName)).count());

      var withoutInternalTopics =
          Assertions.assertInstanceOf(
              TopicHandler.Topics.class,
              handler
                  .get(Channel.ofQueries(Map.of(TopicHandler.LIST_INTERNAL, "false")))
                  .toCompletableFuture()
                  .join());
      Assertions.assertFalse(
          withoutInternalTopics.topics.stream().anyMatch(t -> t.name.equals("__consumer_offsets")));
      Assertions.assertEquals(
          1, withoutInternalTopics.topics.stream().filter(t -> t.name.equals(topicName)).count());
    }
  }

  @Test
  void testCreateTopicWithReplicas() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var request =
          Channel.ofRequest(
              String.format(
                  "{\"topics\":[{\"name\":\"%s\",\"partitions\":\"%s\",\"replicas\":\"%s\", \"configs\":{\"segment.ms\":\"3000\"}}]}",
                  topicName, "2", "2"));
      var topics = handler.post(request).toCompletableFuture().join();
      Assertions.assertEquals(1, topics.topics.size());
      var topicInfo = topics.topics.iterator().next();
      Assertions.assertEquals(topicName, topicInfo.name);

      // the topic creation is not synced, so we have to wait the creation.
      if (topicInfo.partitions.isEmpty()) {
        Utils.sleep(Duration.ofSeconds(2));
        var result =
            admin
                .clusterInfo(Set.of(topicName))
                .toCompletableFuture()
                .join()
                .replicaStream()
                .collect(
                    Collectors.groupingBy(
                        replica -> TopicPartition.of(replica.topic(), replica.partition())));
        Assertions.assertEquals(2, result.size());
        result.values().forEach(replicas -> Assertions.assertEquals(2, replicas.size()));
      } else {
        Assertions.assertEquals(2, topicInfo.partitions.size());
        Assertions.assertEquals(2, topicInfo.partitions.iterator().next().replicas.size());
      }
      Assertions.assertEquals(
          "3000",
          admin.topics(Set.of(topicName)).toCompletableFuture().join().stream()
              .filter(t -> t.name().equals(topicName))
              .findFirst()
              .get()
              .config()
              .value("segment.ms")
              .get());
    }
  }

  @Test
  void testDeleteTopic() {
    var topicNames =
        IntStream.range(0, 3).mapToObj(x -> Utils.randomString(10)).collect(Collectors.toList());
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      for (var name : topicNames)
        admin
            .creator()
            .topic(name)
            .numberOfPartitions(3)
            .numberOfReplicas((short) 3)
            .run()
            .toCompletableFuture()
            .join();
      Utils.sleep(Duration.ofSeconds(2));

      handler.delete(Channel.ofTarget(topicNames.get(0))).toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));

      var latestTopicNames = admin.topicNames(true).toCompletableFuture().join();
      Assertions.assertFalse(latestTopicNames.contains(topicNames.get(0)));
      Assertions.assertTrue(latestTopicNames.contains(topicNames.get(1)));
      Assertions.assertTrue(latestTopicNames.contains(topicNames.get(2)));

      handler.delete(Channel.ofTarget(topicNames.get(2))).toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));

      latestTopicNames = admin.topicNames(true).toCompletableFuture().join();
      Assertions.assertFalse(latestTopicNames.contains(topicNames.get(2)));
      Assertions.assertTrue(latestTopicNames.contains(topicNames.get(1)));
    }
  }

  @Test
  void testGroupIdAndTimestamp() {
    var topicName = Utils.randomString();
    var groupId = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers());
        var consumer =
            Consumer.forTopics(Set.of(topicName))
                .config(ConsumerConfigs.GROUP_ID_CONFIG, groupId)
                .config(
                    ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                    ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
                .bootstrapServers(bootstrapServers())
                .build()) {
      var handler = new TopicHandler(admin);

      producer
          .send(Record.builder().topic(topicName).key(new byte[100]).build())
          .toCompletableFuture()
          .join();

      // try poll
      Assertions.assertEquals(1, consumer.poll(1, Duration.ofSeconds(5)).size());
      consumer.commitOffsets(Duration.ofSeconds(2));
      Assertions.assertEquals(1, consumer.assignments().size());

      var response =
          Assertions.assertInstanceOf(
              TopicHandler.TopicInfo.class,
              handler
                  .get(
                      Channel.builder()
                          .target(topicName)
                          .queries(Map.of(TopicHandler.POLL_RECORD_TIMEOUT, "3s"))
                          .build())
                  .toCompletableFuture()
                  .join());
      Assertions.assertEquals(1, response.activeGroupIds.size());
      Assertions.assertEquals(groupId, response.activeGroupIds.iterator().next());
      Assertions.assertEquals(1, response.partitions.size());
      Assertions.assertNotNull(response.partitions.get(0).maxTimestamp);
      Assertions.assertNotNull(response.partitions.get(0).timestampOfLatestRecord);
    }
  }
}
