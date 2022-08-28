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
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.Utils;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ThrottleHandlerTest extends RequireBrokerCluster {

  @Test
  void testThrottleBandwidth() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var dataRate = DataRate.MiB.of(500).perSecond();
      var longDataRate = (long) dataRate.byteRate();
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
  void runPost() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      admin.creator().topic("MyTopicA").numberOfPartitions(2).numberOfReplicas((short) 3).create();
      admin.creator().topic("MyTopicB").numberOfPartitions(3).numberOfReplicas((short) 3).create();
      admin.creator().topic("MyTopicC").numberOfPartitions(4).numberOfReplicas((short) 3).create();
      admin.creator().topic("MyTopicD").numberOfPartitions(5).numberOfReplicas((short) 3).create();

      var string =
          "{\"brokers\":["
              + "{\"id\":0,\"ingress\":1000,\"egress\":1000},"
              + "{\"id\":1,\"ingress\":1000}],"
              + "\"topics\":["
              + "{\"name\":\"MyTopicA\"},"
              + "{\"name\":\"MyTopicB\",\"partition\":2},"
              + "{\"name\":\"MyTopicC\",\"partition\":3,\"broker\":0},"
              + "{\"name\":\"MyTopicD\",\"partition\":4,\"broker\":0,\"type\":\"leader\"}]}";
      handler.post(Channel.ofRequest(PostRequest.of(string)));
      Utils.sleep(Duration.ofSeconds(1));

      System.out.println("Result 0:");
      System.out.println(handler.get(Channel.EMPTY).json());
      System.out.println();

      handler.delete(Channel.ofQueries(Map.of("topic", "MyTopicA")));
      Utils.sleep(Duration.ofSeconds(1));

      System.out.println("Result 1:");
      System.out.println(handler.get(Channel.EMPTY).json());
      System.out.println();
    }
  }
}
