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

import static org.astraea.app.web.ThrottleHandler.ThrottleBandwidths.egress;
import static org.astraea.app.web.ThrottleHandler.ThrottleBandwidths.ingress;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.Utils;
import org.astraea.app.common.json.OptionalIntTypeAdapter;
import org.astraea.app.common.json.OptionalStringTypeAdapter;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ThrottleHandlerTest extends RequireBrokerCluster {

  @Test
  void testThrottleBandwidth() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var dataRate = DataRate.MiB.of(500).perSecond();
      admin
          .replicationThrottler()
          .ingress(Map.of(0, dataRate, 2, dataRate))
          .egress(Map.of(1, dataRate, 2, dataRate))
          .apply();
      Utils.sleep(Duration.ofSeconds(1));

      var jsonString = handler.get(Channel.EMPTY).json();
      var json = new Gson().fromJson(jsonString, JsonObject.class);

      // broker 0
      Assertions.assertEquals(
          (long) dataRate.byteRate(),
          json.getAsJsonObject("brokers").getAsJsonObject("0").get("ingress").getAsLong());
      Assertions.assertFalse(
          json.getAsJsonObject("brokers").getAsJsonObject("0").keySet().contains("egress"));

      // broker 1
      Assertions.assertFalse(
          json.getAsJsonObject("brokers").getAsJsonObject("1").keySet().contains("ingress"));
      Assertions.assertEquals(
          (long) dataRate.byteRate(),
          json.getAsJsonObject("brokers").getAsJsonObject("1").get("egress").getAsLong());

      // broker 2
      Assertions.assertEquals(
          (long) dataRate.byteRate(),
          json.getAsJsonObject("brokers").getAsJsonObject("2").get("ingress").getAsLong());
      Assertions.assertEquals(
          (long) dataRate.byteRate(),
          json.getAsJsonObject("brokers").getAsJsonObject("2").get("egress").getAsLong());
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
                  .filter(r -> r.broker() == theReplica)
                  .findFirst()
                  .orElseThrow()
                  .leader();
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
    var target0 = new ThrottleHandler.ThrottleTarget("Topic", 0, 0);
    var target1 = new ThrottleHandler.ThrottleTarget("Topic", 0, 0);
    var target2 = new ThrottleHandler.ThrottleTarget("Topic", 0, 0, "leader");
    var target3 = new ThrottleHandler.ThrottleTarget("Topic", 0, 0, "follower");
    var target4 = new ThrottleHandler.ThrottleTarget("Topic", 1, 0);
    var target5 = new ThrottleHandler.ThrottleTarget("Topic2", 0, 0);

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
    var throttle0 = new ThrottleHandler.ThrottleTarget("MyTopic", 0, 1001);
    var throttle1 = new ThrottleHandler.ThrottleTarget("MyTopic", 0, 1002);
    var throttle2 = new ThrottleHandler.ThrottleTarget("MyTopic", 0, 1003);
    var map = Map.of(1001, Map.of(ingress, 1L));
    var set = Set.of(throttle0, throttle1, throttle2);
    var setting = new ThrottleHandler.ThrottleSetting(map, set);

    var serialized = setting.json();
    var gson =
        new GsonBuilder()
            .registerTypeAdapter(OptionalInt.class, new OptionalIntTypeAdapter())
            .registerTypeAdapter(Optional.class, new OptionalStringTypeAdapter())
            .create();
    var deserialized = gson.fromJson(serialized, ThrottleHandler.ThrottleSetting.class);

    Assertions.assertEquals(map, deserialized.brokers);
    Assertions.assertEquals(set, Set.copyOf(deserialized.topics));
  }

  @Test
  void testDeserialize() {
    final String rawJson =
        "{\"brokers\":{"
            + "\"1001\":{\"ingress\":1000,\"egress\":1000},"
            + "\"1002\":{\"ingress\":1000}},"
            + "\"topics\":["
            + "{\"name\":\"MyTopicA\"},"
            + "{\"name\":\"MyTopicB\",\"partition\":2},"
            + "{\"name\":\"MyTopicC\",\"partition\":3,\"broker\":1001},"
            + "{\"name\":\"MyTopicD\",\"partition\":4,\"broker\":1001,\"type\":\"leader\"}]}";
    var expectedMap =
        Map.of(
            1001, Map.of(ingress, 1000L, egress, 1000L),
            1002, Map.of(ingress, 1000L));
    var expectedSet =
        Set.of(
            new ThrottleHandler.ThrottleTarget("MyTopicA"),
            new ThrottleHandler.ThrottleTarget("MyTopicB", 2),
            new ThrottleHandler.ThrottleTarget("MyTopicC", 3, 1001),
            new ThrottleHandler.ThrottleTarget("MyTopicD", 4, 1001, "leader"));

    var gson =
        new GsonBuilder()
            .registerTypeAdapter(OptionalInt.class, new OptionalIntTypeAdapter())
            .registerTypeAdapter(Optional.class, new OptionalStringTypeAdapter())
            .create();
    var deserialized = gson.fromJson(rawJson, ThrottleHandler.ThrottleSetting.class);

    Assertions.assertEquals(expectedMap, deserialized.brokers);
    Assertions.assertEquals(expectedSet, Set.copyOf(deserialized.topics));
  }
}
