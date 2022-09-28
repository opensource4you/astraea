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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.astraea.common.DataRate;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Node;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ThrottleHandlerTest extends RequireBrokerCluster {

  @Test
  void testThrottleBandwidth() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var dataRate = DataRate.MiB.of(500).perSecond();
      var longDataRate = (long) dataRate.byteRate();

      // other tests might write this value too, ensure it is clean before we start
      admin.clearIngressReplicationThrottle(brokerIds());
      admin.clearEgressReplicationThrottle(brokerIds());
      Utils.sleep(Duration.ofSeconds(1));

      admin
          .replicationThrottler()
          .ingress(Map.of(0, dataRate, 2, dataRate))
          .egress(Map.of(1, dataRate, 2, dataRate))
          .apply();
      Utils.sleep(Duration.ofSeconds(1));

      var jsonString = handler.get(Channel.EMPTY).json();
      var json = new Gson().fromJson(jsonString, JsonObject.class);

      Function<Integer, JsonObject> findByBrokerId =
          (brokerId) ->
              StreamSupport.stream(json.getAsJsonArray("brokers").spliterator(), false)
                  .map(JsonElement::getAsJsonObject)
                  .filter(item -> item.get("id").getAsInt() == brokerId)
                  .findFirst()
                  .orElseThrow();

      // broker 0
      Assertions.assertEquals(longDataRate, findByBrokerId.apply(0).get("ingress").getAsLong());
      Assertions.assertFalse(findByBrokerId.apply(0).keySet().contains("egress"));

      // broker 1
      Assertions.assertFalse(findByBrokerId.apply(1).keySet().contains("ingress"));
      Assertions.assertEquals(longDataRate, findByBrokerId.apply(1).get("egress").getAsLong());

      // broker 2
      Assertions.assertEquals(longDataRate, findByBrokerId.apply(2).get("ingress").getAsLong());
      Assertions.assertEquals(longDataRate, findByBrokerId.apply(2).get("egress").getAsLong());
    }
  }

  @Test
  void testThrottleSomeLogs() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var topicName = Utils.randomString();
      admin.creator().topic(topicName).numberOfPartitions(3).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));
      admin.replicationThrottler().throttle(topicName).apply();
      Utils.sleep(Duration.ofSeconds(1));
      var currentReplicas = admin.replicas();

      var jsonString = handler.get(Channel.EMPTY).json();
      var json = new Gson().fromJson(jsonString, JsonObject.class);

      for (int partition = 0; partition < 3; partition++) {
        for (int replica = 0; replica < 3; replica++) {
          var theReplica = replica;
          var isLeader =
              currentReplicas.get(TopicPartition.of(topicName, partition)).stream()
                  .filter(r -> r.nodeInfo().id() == theReplica)
                  .findFirst()
                  .orElseThrow()
                  .isLeader();
          var expected = new JsonObject();
          expected.add("name", new JsonPrimitive(topicName));
          expected.add("partition", new JsonPrimitive(partition));
          expected.add("broker", new JsonPrimitive(replica));
          expected.add("type", new JsonPrimitive(isLeader ? "leader" : "follower"));
          Assertions.assertTrue(json.getAsJsonArray("topics").contains(expected));
        }
      }
    }
  }

  @Test
  void testThrottleEveryLog() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var topicName = Utils.randomString();
      admin.creator().topic(topicName).numberOfPartitions(3).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));
      admin
          .replicationThrottler()
          .throttle(TopicPartitionReplica.of(topicName, 0, 0))
          .throttle(TopicPartitionReplica.of(topicName, 0, 1))
          .throttle(TopicPartitionReplica.of(topicName, 0, 2))
          .throttle(TopicPartitionReplica.of(topicName, 1, 0))
          .throttle(TopicPartitionReplica.of(topicName, 1, 1))
          .throttle(TopicPartitionReplica.of(topicName, 1, 2))
          .throttle(TopicPartitionReplica.of(topicName, 2, 0))
          .throttle(TopicPartitionReplica.of(topicName, 2, 1))
          .throttle(TopicPartitionReplica.of(topicName, 2, 2))
          .apply();
      Utils.sleep(Duration.ofSeconds(1));

      var jsonString = handler.get(Channel.EMPTY).json();
      var json = new Gson().fromJson(jsonString, JsonObject.class);

      for (int partition = 0; partition < 3; partition++) {
        for (int replica = 0; replica < 3; replica++) {
          var expected = new JsonObject();
          expected.add("name", new JsonPrimitive(topicName));
          expected.add("partition", new JsonPrimitive(partition));
          expected.add("broker", new JsonPrimitive(replica));
          Assertions.assertTrue(json.getAsJsonArray("topics").contains(expected));
        }
      }
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
    var gson = new Gson();
    var deserialized = gson.fromJson(serialized, ThrottleHandler.ThrottleSetting.class);

    Assertions.assertEquals(set0, Set.copyOf(deserialized.brokers));
    Assertions.assertEquals(set1, Set.copyOf(deserialized.topics));
  }

  @Test
  void testDeserialize() {
    final String rawJson =
        "{\"brokers\":["
            + "{\"id\": 1001, \"ingress\":1000,\"egress\":1000},"
            + "{\"id\": 1002, \"ingress\":1000}],"
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

    var gson = new Gson();
    var deserialized = gson.fromJson(rawJson, ThrottleHandler.ThrottleSetting.class);

    Assertions.assertEquals(expectedBroker, Set.copyOf(deserialized.brokers));
    Assertions.assertEquals(expectedTopic, Set.copyOf(deserialized.topics));
  }

  @Test
  void testPost() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var topicA = Utils.randomString();
      var topicB = Utils.randomString();
      var topicC = Utils.randomString();
      var topicD = Utils.randomString();
      admin.creator().topic(topicA).numberOfPartitions(2).numberOfReplicas((short) 3).create();
      admin.creator().topic(topicB).numberOfPartitions(3).numberOfReplicas((short) 3).create();
      admin.creator().topic(topicC).numberOfPartitions(4).numberOfReplicas((short) 3).create();
      admin.creator().topic(topicD).numberOfPartitions(5).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));
      admin.migrator().partition(topicA, 0).moveTo(List.of(0, 1, 2));
      admin.migrator().partition(topicA, 1).moveTo(List.of(0, 1, 2));
      admin.migrator().partition(topicB, 2).moveTo(List.of(0, 1, 2));
      admin.migrator().partition(topicC, 3).moveTo(List.of(0, 1, 2));
      admin.migrator().partition(topicD, 4).moveTo(List.of(0, 1, 2));
      Utils.sleep(Duration.ofSeconds(1));
      admin.preferredLeaderElection(TopicPartition.of(topicA, 0));
      admin.preferredLeaderElection(TopicPartition.of(topicA, 1));
      admin.preferredLeaderElection(TopicPartition.of(topicB, 2));
      admin.preferredLeaderElection(TopicPartition.of(topicC, 3));
      admin.preferredLeaderElection(TopicPartition.of(topicD, 4));
      Utils.sleep(Duration.ofSeconds(1));
      var rawJson =
          "{\"brokers\":["
              + "{\"id\":0,\"ingress\":1000,\"egress\":1000},"
              + "{\"id\":1,\"ingress\":1000}],"
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
              new ThrottleHandler.TopicThrottle(topicA, 0, 0, leader),
              new ThrottleHandler.TopicThrottle(topicA, 0, 1, follower),
              new ThrottleHandler.TopicThrottle(topicA, 0, 2, follower),
              new ThrottleHandler.TopicThrottle(topicA, 1, 0, leader),
              new ThrottleHandler.TopicThrottle(topicA, 1, 1, follower),
              new ThrottleHandler.TopicThrottle(topicA, 1, 2, follower),
              new ThrottleHandler.TopicThrottle(topicB, 2, 0, leader),
              new ThrottleHandler.TopicThrottle(topicB, 2, 1, follower),
              new ThrottleHandler.TopicThrottle(topicB, 2, 2, follower),
              new ThrottleHandler.TopicThrottle(topicC, 3, 0, null),
              new ThrottleHandler.TopicThrottle(topicD, 4, 1, follower),
              new ThrottleHandler.TopicThrottle(topicD, 4, 0, leader));
      var affectedLeaders =
          Set.of(
              TopicPartitionReplica.of(topicA, 0, 0),
              TopicPartitionReplica.of(topicA, 1, 0),
              TopicPartitionReplica.of(topicB, 2, 0),
              TopicPartitionReplica.of(topicC, 3, 0),
              TopicPartitionReplica.of(topicD, 4, 0));
      var affectedFollowers =
          Set.of(
              TopicPartitionReplica.of(topicA, 0, 1),
              TopicPartitionReplica.of(topicA, 0, 2),
              TopicPartitionReplica.of(topicA, 1, 1),
              TopicPartitionReplica.of(topicA, 1, 2),
              TopicPartitionReplica.of(topicB, 2, 1),
              TopicPartitionReplica.of(topicB, 2, 2),
              TopicPartitionReplica.of(topicC, 3, 0),
              TopicPartitionReplica.of(topicD, 4, 1));

      var post = handler.post(Channel.ofRequest(PostRequest.of(rawJson)));
      var deserialized = new Gson().fromJson(post.json(), ThrottleHandler.ThrottleSetting.class);
      Utils.sleep(Duration.ofSeconds(1));

      // verify response content is correct
      Assertions.assertEquals(200, post.code());
      Assertions.assertEquals(affectedBrokers, Set.copyOf(deserialized.brokers));
      Assertions.assertEquals(affectedTopics, Set.copyOf(deserialized.topics));

      // verify topic/broker configs are correct
      final var topicConfigs = admin.topics(Set.of(topicA, topicB, topicC, topicD));
      Assertions.assertTrue(
          affectedLeaders.stream()
              .allMatch(
                  log ->
                      topicConfigs.stream()
                          .filter(t -> t.name().equals(log.topic()))
                          .findFirst()
                          .get()
                          .config()
                          .value("leader.replication.throttled.replicas")
                          .orElse("")
                          .contains(log.partition() + ":" + log.brokerId())));
      Assertions.assertTrue(
          affectedFollowers.stream()
              .allMatch(
                  log ->
                      topicConfigs.stream()
                          .filter(t -> t.name().equals(log.topic()))
                          .findFirst()
                          .get()
                          .config()
                          .value("follower.replication.throttled.replicas")
                          .orElse("")
                          .contains(log.partition() + ":" + log.brokerId())));
      final var brokerConfigs =
          admin.nodes().stream().collect(Collectors.toMap(Node::id, Function.identity()));
      Assertions.assertEquals(
          1000L,
          brokerConfigs
              .get(0)
              .config()
              .value("leader.replication.throttled.rate")
              .map(Long::parseLong)
              .orElse(0L));
      Assertions.assertEquals(
          1000L,
          brokerConfigs
              .get(0)
              .config()
              .value("follower.replication.throttled.rate")
              .map(Long::parseLong)
              .orElse(0L));
      Assertions.assertEquals(
          1000L,
          brokerConfigs
              .get(1)
              .config()
              .value("follower.replication.throttled.rate")
              .map(Long::parseLong)
              .orElse(0L));
    }
  }

  @Test
  void testBadPost() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);

      // empty
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> handler.post(Channel.ofRequest(PostRequest.of("{\"topics\":[{}]}"))));

      // no key "name" specified
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler.post(Channel.ofRequest(PostRequest.of("{\"topics\":[{\"partition\": 3}]}"))));

      // this key combination is not supported
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler.post(
                  Channel.ofRequest(
                      PostRequest.of("{\"topics\":[{\"name\": \"A\", \"broker\": 3}]}"))));

      // illegal type value
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler.post(
                  Channel.ofRequest(
                      PostRequest.of(
                          "{\"topics\":[{\"name\": \"A\", \"partition\": 0, \"broker\": 3, \"type\": \"owo?\"}]}"))));
    }
  }

  @Test
  void testDelete() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var topic = Utils.randomString();
      admin.creator().topic(topic).numberOfPartitions(3).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofMillis(500));
      admin.migrator().partition(topic, 0).moveTo(List.of(0, 1, 2));
      admin.migrator().partition(topic, 1).moveTo(List.of(0, 1, 2));
      admin.migrator().partition(topic, 2).moveTo(List.of(0, 1, 2));
      Utils.sleep(Duration.ofMillis(500));
      admin.preferredLeaderElection(TopicPartition.of(topic, 0));
      admin.preferredLeaderElection(TopicPartition.of(topic, 1));
      admin.preferredLeaderElection(TopicPartition.of(topic, 2));
      Utils.sleep(Duration.ofMillis(500));

      Supplier<String> leaderConfig =
          () ->
              admin
                  .topics(Set.of(topic))
                  .get(0)
                  .config()
                  .value("leader.replication.throttled.replicas")
                  .orElse("");
      Supplier<String> followerConfig =
          () ->
              admin
                  .topics(Set.of(topic))
                  .get(0)
                  .config()
                  .value("follower.replication.throttled.replicas")
                  .orElse("");
      Function<Integer, Long> egressRate =
          (id) ->
              admin.nodes().stream()
                  .filter(n -> n.id() == id)
                  .findFirst()
                  .get()
                  .config()
                  .value("leader.replication.throttled.rate")
                  .map(Long::parseLong)
                  .orElse(-1L);
      Function<Integer, Long> ingressRate =
          (id) ->
              admin.nodes().stream()
                  .filter(n -> n.id() == id)
                  .findFirst()
                  .get()
                  .config()
                  .value("follower.replication.throttled.rate")
                  .map(Long::parseLong)
                  .orElse(-1L);
      Runnable setThrottle =
          () -> {
            admin
                .replicationThrottler()
                .throttle(topic)
                .ingress(DataRate.Byte.of(100).perSecond())
                .egress(DataRate.Byte.of(100).perSecond())
                .apply();
            Utils.sleep(Duration.ofMillis(500));
          };

      // delete topic
      setThrottle.run();
      int code0 = handler.delete(Channel.ofQueries(Map.of("topic", topic))).code();
      Utils.sleep(Duration.ofMillis(500));
      Assertions.assertEquals(202, code0);
      Assertions.assertEquals("", leaderConfig.get());
      Assertions.assertEquals("", followerConfig.get());

      // delete topic/partition
      setThrottle.run();
      int code1 =
          handler.delete(Channel.ofQueries(Map.of("topic", topic, "partition", "0"))).code();
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
                          "type", "ingress")))
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
                          "type", "egress")))
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
                          "type", "ingress+egress")))
              .code();
      Utils.sleep(Duration.ofMillis(500));
      Assertions.assertEquals(202, code7);
      Assertions.assertEquals(-1L, egressRate.apply(0));
      Assertions.assertEquals(-1L, ingressRate.apply(0));
    }
  }

  @Test
  void testBadDelete() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);

      // empty
      Assertions.assertEquals(
          Response.BAD_REQUEST.code(), handler.delete(Channel.ofQueries(Map.of())).code());

      // no key "topic" specified
      Assertions.assertEquals(
          Response.BAD_REQUEST.code(),
          handler.delete(Channel.ofQueries(Map.of("partition", "0"))).code());

      // this key combination is not supported
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler.delete(
                  Channel.ofQueries(
                      Map.of(
                          "topic", "MyTopic",
                          "replica", "0"))));

      // illegal type value
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler.delete(
                  Channel.ofQueries(
                      Map.of(
                          "topic", "MyTopic",
                          "partition", "0",
                          "replica", "0",
                          "type", "owo?"))));

      // illegal clear target
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler.delete(
                  Channel.ofQueries(
                      Map.of(
                          "broker", "0",
                          "type", "ingress+egress+everyTopic"))));
    }
  }
}
