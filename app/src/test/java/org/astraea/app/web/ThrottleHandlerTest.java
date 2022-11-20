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

import static org.astraea.app.web.ThrottleHandler.LogIdentity.follower;
import static org.astraea.app.web.ThrottleHandler.LogIdentity.leader;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.DataRate;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.BrokerConfigs;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.TopicConfigs;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ThrottleHandlerTest extends RequireBrokerCluster {

  @BeforeEach
  public void cleanup() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .nodeInfos()
          .thenApply(
              ns ->
                  ns.stream()
                      .collect(
                          Collectors.toMap(
                              NodeInfo::id,
                              ignored ->
                                  Set.of(
                                      BrokerConfigs.LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
                                      BrokerConfigs.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG))))
          .thenCompose(admin::unsetBrokerConfigs)
          .toCompletableFuture()
          .join();

      admin
          .topicNames(true)
          .thenApply(
              names ->
                  names.stream()
                      .collect(
                          Collectors.toMap(
                              n -> n,
                              ignored ->
                                  Set.of(
                                      TopicConfigs.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
                                      TopicConfigs
                                          .FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG))))
          .thenCompose(admin::unsetTopicConfigs)
          .toCompletableFuture()
          .join();
    }
  }

  @Test
  void testThrottleBandwidth() {
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var dataRate = DataRate.MiB.of(500).perSecond();
      var longDataRate = (long) dataRate.byteRate();

      Utils.sleep(Duration.ofSeconds(1));

      admin
          .setBrokerConfigs(
              Map.of(
                  0,
                  Map.of(
                      BrokerConfigs.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG,
                      String.valueOf(longDataRate)),
                  1,
                  Map.of(
                      BrokerConfigs.LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
                      String.valueOf(longDataRate)),
                  2,
                  Map.of(
                      BrokerConfigs.LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
                      String.valueOf(longDataRate),
                      BrokerConfigs.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG,
                      String.valueOf(longDataRate))))
          .toCompletableFuture()
          .join();

      Utils.sleep(Duration.ofSeconds(1));

      var throttleSetting =
          Assertions.assertInstanceOf(
              ThrottleHandler.ThrottleSetting.class,
              handler.get(Channel.EMPTY).toCompletableFuture().join());

      Assertions.assertEquals(3, throttleSetting.brokers.size());
      var brokerThrottle =
          throttleSetting.brokers.stream()
              .collect(Collectors.groupingBy(b -> b.id))
              .entrySet()
              .stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
      Assertions.assertEquals(longDataRate, brokerThrottle.get(0).follower.orElse(null));
      Assertions.assertNull(brokerThrottle.get(0).leader.orElse(null));

      Assertions.assertEquals(longDataRate, brokerThrottle.get(1).leader.orElse(null));
      Assertions.assertNull(brokerThrottle.get(1).follower.orElse(null));

      Assertions.assertEquals(longDataRate, brokerThrottle.get(2).follower.orElse(null));
      Assertions.assertEquals(longDataRate, brokerThrottle.get(2).leader.orElse(null));
    }
  }

  @Test
  void testThrottleSomeLogs() {
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var topicName = Utils.randomString();
      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));

      admin
          .setTopicConfigs(
              Map.of(
                  topicName,
                  Map.of(
                      TopicConfigs.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
                      "1:2",
                      TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
                      "0:1")))
          .toCompletableFuture()
          .join();

      var throttleSetting =
          Assertions.assertInstanceOf(
              ThrottleHandler.ThrottleSetting.class,
              handler.get(Channel.EMPTY).toCompletableFuture().join());

      var topic =
          throttleSetting.topics.stream()
              .filter(t -> t.name.get().equals(topicName))
              .collect(Collectors.toList());
      Assertions.assertEquals(2, topic.size());

      var leader =
          topic.stream().filter(t -> "leader".equals(t.type.orElse(null))).findFirst().get();
      Assertions.assertEquals(1, leader.partition.get());
      Assertions.assertEquals(2, leader.broker.get());

      var follower =
          topic.stream().filter(t -> "follower".equals(t.type.orElse(null))).findFirst().get();
      Assertions.assertEquals(0, follower.partition.get());
      Assertions.assertEquals(1, follower.broker.get());
    }
  }

  @Test
  void testThrottleEveryLog() {
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var topicName = Utils.randomString();
      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));

      var value = "0:0,0:1,0:2,1:0,1:1,1:2,2:0,2:1,2:2";

      admin
          .setTopicConfigs(
              Map.of(
                  topicName,
                  Map.of(
                      TopicConfigs.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
                      value,
                      TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
                      value)))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));

      var throttleSetting =
          Assertions.assertInstanceOf(
              ThrottleHandler.ThrottleSetting.class,
              handler.get(Channel.EMPTY).toCompletableFuture().join());
      var topic =
          throttleSetting.topics.stream()
              .filter(t -> t.name.get().equals(topicName))
              .collect(Collectors.toList());
      Assertions.assertEquals(9, topic.size());

      IntStream.range(0, 3)
          .forEach(
              partition ->
                  IntStream.range(0, 3)
                      .forEach(
                          replica ->
                              Assertions.assertTrue(
                                  topic.stream()
                                      .anyMatch(
                                          t ->
                                              t.partition.orElse(null) == partition
                                                  && t.broker.orElse(null) == replica))));
    }
  }

  @Test
  void testThrottleTargetEqual() {
    var target0 = new ThrottleHandler.TopicThrottle("Topic", 0, 0, null);
    var target1 = new ThrottleHandler.TopicThrottle("Topic", 0, 0, null);
    var target2 = new ThrottleHandler.TopicThrottle("Topic", 0, 0, leader);
    var target3 = new ThrottleHandler.TopicThrottle("Topic", 0, 0, follower);
    var target4 = new ThrottleHandler.TopicThrottle("Topic", 1, 0, null);
    var target5 = new ThrottleHandler.TopicThrottle("Topic2", 0, 0, null);

    Assertions.assertEquals(target0, target0);
    Assertions.assertEquals(target0, target1);
    Assertions.assertNotEquals(target0, target2);
    Assertions.assertNotEquals(target0, target3);
    Assertions.assertNotEquals(target2, target3);
    Assertions.assertNotEquals(target0, target4);
    Assertions.assertNotEquals(target0, target5);
    Assertions.assertEquals(target0.hashCode(), target1.hashCode());
  }

  @Test
  void testSerializeDeserialize() {
    var throttle0 = new ThrottleHandler.BrokerThrottle(1001, 1L, null);
    var throttle1 = new ThrottleHandler.TopicThrottle("MyTopic", 0, 1001, null);
    var throttle2 = new ThrottleHandler.TopicThrottle("MyTopic", 0, 1002, null);
    var throttle3 = new ThrottleHandler.TopicThrottle("MyTopic", 0, 1003, null);
    var set0 = Set.of(throttle0);
    var set1 = Set.of(throttle1, throttle2, throttle3);
    var setting = new ThrottleHandler.ThrottleSetting(set0, set1);

    var serialized = setting.json();
    var deserialized =
        JsonConverter.defaultConverter()
            .fromJson(serialized, TypeRef.of(ThrottleHandler.ThrottleSetting.class));

    Assertions.assertEquals(set0, Set.copyOf(deserialized.brokers));
    Assertions.assertEquals(set1, Set.copyOf(deserialized.topics));
  }

  @Test
  void testDeserialize() {
    final String rawJson =
        "{\"brokers\":["
            + "{\"id\": 1001, \"follower\":1000,\"leader\":1000},"
            + "{\"id\": 1002, \"follower\":1000}],"
            + "\"topics\":["
            + "{\"name\":\"MyTopicA\"},"
            + "{\"name\":\"MyTopicB\",\"partition\":2},"
            + "{\"name\":\"MyTopicC\",\"partition\":3,\"broker\":1001},"
            + "{\"name\":\"MyTopicD\",\"partition\":4,\"broker\":1001,\"type\":\"leader\"}]}";
    var expectedBroker =
        Set.of(
            new ThrottleHandler.BrokerThrottle(1001, 1000L, 1000L),
            new ThrottleHandler.BrokerThrottle(1002, 1000L, null));
    var expectedTopic =
        Set.of(
            new ThrottleHandler.TopicThrottle("MyTopicA", null, null, null),
            new ThrottleHandler.TopicThrottle("MyTopicB", 2, null, null),
            new ThrottleHandler.TopicThrottle("MyTopicC", 3, 1001, null),
            new ThrottleHandler.TopicThrottle("MyTopicD", 4, 1001, leader));

    var deserialized =
        JsonConverter.defaultConverter()
            .fromJson(rawJson, TypeRef.of(ThrottleHandler.ThrottleSetting.class));

    Assertions.assertEquals(expectedBroker, Set.copyOf(deserialized.brokers));
    Assertions.assertEquals(expectedTopic, Set.copyOf(deserialized.topics));
  }

  @Test
  void testPost() {
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var topicA = Utils.randomString();
      var topicB = Utils.randomString();
      var topicC = Utils.randomString();
      var topicD = Utils.randomString();
      admin
          .creator()
          .topic(topicA)
          .numberOfPartitions(2)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      admin
          .creator()
          .topic(topicB)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      admin
          .creator()
          .topic(topicC)
          .numberOfPartitions(4)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      admin
          .creator()
          .topic(topicD)
          .numberOfPartitions(5)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      admin
          .moveToBrokers(
              Map.of(
                  TopicPartition.of(topicA, 0),
                  List.of(0, 1, 2),
                  TopicPartition.of(topicA, 1),
                  List.of(0, 1, 2),
                  TopicPartition.of(topicB, 2),
                  List.of(0, 1, 2),
                  TopicPartition.of(topicC, 3),
                  List.of(0, 1, 2),
                  TopicPartition.of(topicD, 4),
                  List.of(0, 1, 2)))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      admin
          .preferredLeaderElection(
              Set.of(
                  TopicPartition.of(topicA, 0),
                  TopicPartition.of(topicA, 1),
                  TopicPartition.of(topicB, 2),
                  TopicPartition.of(topicC, 3),
                  TopicPartition.of(topicD, 4)))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      var rawJson =
          "{\"brokers\":["
              + "{\"id\":0,\"follower\":1000,\"leader\":1000},"
              + "{\"id\":1,\"follower\":1000}],"
              + "\"topics\":["
              + "{\"name\":\""
              + topicA
              + "\"},"
              + "{\"name\":\""
              + topicB
              + "\",\"partition\":2},"
              + "{\"name\":\""
              + topicC
              + "\",\"partition\":3,\"broker\":0},"
              + "{\"name\":\""
              + topicD
              + "\",\"partition\":4,\"broker\":1,\"type\":\"follower\"},"
              + "{\"name\":\""
              + topicD
              + "\",\"partition\":4,\"broker\":0,\"type\":\"leader\"}]}";
      var affectedBrokers =
          Set.of(
              new ThrottleHandler.BrokerThrottle(0, 1000L, 1000L),
              new ThrottleHandler.BrokerThrottle(1, 1000L, null));
      var affectedTopics =
          Set.of(
              new ThrottleHandler.TopicThrottle(topicA, 0, 0, null),
              new ThrottleHandler.TopicThrottle(topicA, 0, 1, null),
              new ThrottleHandler.TopicThrottle(topicA, 0, 2, null),
              new ThrottleHandler.TopicThrottle(topicA, 1, 0, null),
              new ThrottleHandler.TopicThrottle(topicA, 1, 1, null),
              new ThrottleHandler.TopicThrottle(topicA, 1, 2, null),
              new ThrottleHandler.TopicThrottle(topicB, 2, 0, null),
              new ThrottleHandler.TopicThrottle(topicB, 2, 1, null),
              new ThrottleHandler.TopicThrottle(topicB, 2, 2, null),
              new ThrottleHandler.TopicThrottle(topicC, 3, 0, null),
              new ThrottleHandler.TopicThrottle(topicD, 4, 1, follower),
              new ThrottleHandler.TopicThrottle(topicD, 4, 0, leader));

      var post = handler.post(Channel.ofRequest(rawJson)).toCompletableFuture().join();
      Assertions.assertEquals(202, post.code());

      Utils.sleep(Duration.ofSeconds(5));
      var deserialized = handler.get(Channel.EMPTY).toCompletableFuture().join();
      // verify response content is correct
      Assertions.assertEquals(affectedBrokers, Set.copyOf(deserialized.brokers));
      Assertions.assertEquals(affectedTopics, Set.copyOf(deserialized.topics));
    }
  }

  @Test
  void testDelete() {
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var topic = Utils.randomString();
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofMillis(500));
      admin
          .moveToBrokers(
              Map.of(
                  TopicPartition.of(topic, 0),
                  List.of(0, 1, 2),
                  TopicPartition.of(topic, 1),
                  List.of(0, 1, 2),
                  TopicPartition.of(topic, 2),
                  List.of(0, 1, 2)))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofMillis(500));
      admin
          .preferredLeaderElection(
              Set.of(
                  TopicPartition.of(topic, 0),
                  TopicPartition.of(topic, 1),
                  TopicPartition.of(topic, 2)))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofMillis(500));

      Supplier<String> leaderConfig =
          () ->
              admin
                  .topics(Set.of(topic))
                  .toCompletableFuture()
                  .join()
                  .get(0)
                  .config()
                  .value(TopicConfigs.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
                  .orElse("");
      Supplier<String> followerConfig =
          () ->
              admin
                  .topics(Set.of(topic))
                  .toCompletableFuture()
                  .join()
                  .get(0)
                  .config()
                  .value(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
                  .orElse("");
      Function<Integer, Long> egressRate =
          (id) ->
              admin.brokers().toCompletableFuture().join().stream()
                  .filter(n -> n.id() == id)
                  .findFirst()
                  .get()
                  .config()
                  .value(BrokerConfigs.LEADER_REPLICATION_THROTTLED_RATE_CONFIG)
                  .map(Long::parseLong)
                  .orElse(-1L);
      Function<Integer, Long> ingressRate =
          (id) ->
              admin.brokers().toCompletableFuture().join().stream()
                  .filter(n -> n.id() == id)
                  .findFirst()
                  .get()
                  .config()
                  .value(BrokerConfigs.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)
                  .map(Long::parseLong)
                  .orElse(-1L);
      Runnable setThrottle =
          () -> {
            admin
                .nodeInfos()
                .thenApply(
                    ns ->
                        ns.stream()
                            .map(NodeInfo::id)
                            .collect(
                                Collectors.toMap(
                                    n -> n,
                                    ignored ->
                                        Map.of(
                                            BrokerConfigs
                                                .FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG,
                                            "100",
                                            BrokerConfigs.LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
                                            "100"))))
                .thenCompose(admin::setBrokerConfigs)
                .toCompletableFuture()
                .join();
            Utils.sleep(Duration.ofMillis(500));
          };

      // delete topic
      setThrottle.run();
      int code0 =
          handler
              .delete(Channel.ofQueries(Map.of("topic", topic)))
              .toCompletableFuture()
              .join()
              .code();
      Utils.sleep(Duration.ofMillis(500));
      Assertions.assertEquals(202, code0);
      Assertions.assertEquals("", leaderConfig.get());
      Assertions.assertEquals("", followerConfig.get());

      // delete topic/partition
      setThrottle.run();
      int code1 =
          handler
              .delete(Channel.ofQueries(Map.of("topic", topic, "partition", "0")))
              .toCompletableFuture()
              .join()
              .code();
      Utils.sleep(Duration.ofMillis(500));
      Assertions.assertEquals(202, code1);
      Assertions.assertFalse(leaderConfig.get().matches("0:[0-9]+"));
      Assertions.assertFalse(followerConfig.get().matches("0:[0-9]+"));

      // delete topic/partition/replica
      setThrottle.run();
      int code2 =
          handler
              .delete(
                  Channel.ofQueries(
                      Map.of(
                          "topic", topic,
                          "partition", "0",
                          "replica", "0")))
              .toCompletableFuture()
              .join()
              .code();
      Utils.sleep(Duration.ofMillis(500));
      Assertions.assertEquals(202, code2);
      Assertions.assertFalse(leaderConfig.get().matches("0:0"));
      Assertions.assertFalse(followerConfig.get().matches("0:0"));

      // delete topic/partition/replica/type
      setThrottle.run();
      int code3 =
          handler
              .delete(
                  Channel.ofQueries(
                      Map.of(
                          "topic", topic,
                          "partition", "0",
                          "replica", "0",
                          "type", "leader")))
              .toCompletableFuture()
              .join()
              .code();
      int code4 =
          handler
              .delete(
                  Channel.ofQueries(
                      Map.of(
                          "topic", topic,
                          "partition", "0",
                          "replica", "1",
                          "type", "follower")))
              .toCompletableFuture()
              .join()
              .code();
      Utils.sleep(Duration.ofMillis(500));
      Assertions.assertEquals(202, code3);
      Assertions.assertEquals(202, code4);
      Assertions.assertFalse(leaderConfig.get().matches("0:0"));
      Assertions.assertFalse(followerConfig.get().matches("0:1"));

      // delete broker/type=ingress
      setThrottle.run();
      int code5 =
          handler
              .delete(
                  Channel.ofQueries(
                      Map.of(
                          "broker", "0",
                          "type", "follower")))
              .toCompletableFuture()
              .join()
              .code();
      Utils.sleep(Duration.ofMillis(500));
      Assertions.assertEquals(202, code5);
      Assertions.assertEquals(100L, egressRate.apply(0));
      Assertions.assertEquals(-1L, ingressRate.apply(0));

      // delete broker/type=egress
      setThrottle.run();
      int code6 =
          handler
              .delete(
                  Channel.ofQueries(
                      Map.of(
                          "broker", "0",
                          "type", "leader")))
              .toCompletableFuture()
              .join()
              .code();
      Utils.sleep(Duration.ofMillis(500));
      Assertions.assertEquals(202, code6);
      Assertions.assertEquals(-1L, egressRate.apply(0));
      Assertions.assertEquals(100L, ingressRate.apply(0));

      // delete broker/type=ingress+egress
      setThrottle.run();
      int code7 =
          handler
              .delete(
                  Channel.ofQueries(
                      Map.of(
                          "broker", "0",
                          "type", "follower+leader")))
              .toCompletableFuture()
              .join()
              .code();
      Utils.sleep(Duration.ofMillis(500));
      Assertions.assertEquals(202, code7);
      Assertions.assertEquals(-1L, egressRate.apply(0));
      Assertions.assertEquals(-1L, ingressRate.apply(0));
    }
  }
}
