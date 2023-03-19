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
package org.astraea.common.serializer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.Node;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.backup.ByteUtils;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.producer.Serializer;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ClusterInfoSerializerTest {
  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testNodeInfoSerializer() {
    var nodeInfo = NodeInfo.of(new Node(10, "host", 100));
    var serializedNodeInfo =
        Serializer.NODE_INFO.serialize("topic", Collections.emptyList(), nodeInfo);
    var buffer = ByteBuffer.wrap(serializedNodeInfo);

    Assertions.assertEquals(10, buffer.getInt());
    Assertions.assertEquals("host", ByteUtils.readString(buffer, buffer.getShort()));
    Assertions.assertEquals(100, buffer.getInt());
  }

  @Test
  void testNodeInfoDeserializer() {
    var nodeInfo = NodeInfo.of(new Node(10, "host", 100));
    var serializedNodeInfo =
        Serializer.NODE_INFO.serialize("topic", Collections.emptyList(), nodeInfo);
    var deserializedNodeInfo =
        Deserializer.NODE_INFO.deserialize("topic", Collections.emptyList(), serializedNodeInfo);

    Assertions.assertEquals(nodeInfo, deserializedNodeInfo);
  }

  @Test
  void testTopicSerializer() {
    var topicName = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      var topic = admin.topics(Set.of(topicName)).toCompletableFuture().join().get(0);
      var serializedTopic = Serializer.TOPIC.serialize("topic", Collections.emptyList(), topic);
      var buffer = ByteBuffer.wrap(serializedTopic);

      Assertions.assertEquals(topic.name(), ByteUtils.readString(buffer, buffer.getShort()));
      var config =
          IntStream.range(0, buffer.getInt())
              .boxed()
              .collect(
                  Collectors.toMap(
                      i -> ByteUtils.readString(buffer, buffer.getShort()),
                      i -> ByteUtils.readString(buffer, buffer.getShort())));

      Assertions.assertEquals(topic.config().raw(), config);
      Assertions.assertEquals(topic.internal(), buffer.get() != 0);
      var topicPartitions =
          IntStream.range(0, buffer.getInt())
              .mapToObj(i -> TopicPartition.of(topicName, buffer.getInt()))
              .collect(Collectors.toSet());

      Assertions.assertEquals(topic.topicPartitions(), topicPartitions);
    }
  }

  @Test
  void testTopicDeserializer() {
    var topicName = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      var topic = admin.topics(Set.of(topicName)).toCompletableFuture().join().get(0);
      var serializedTopic = Serializer.TOPIC.serialize("topic", Collections.emptyList(), topic);
      var deserializedTopic =
          Deserializer.TOPIC.deserialize("topic", Collections.emptyList(), serializedTopic);

      Assertions.assertEquals(topic.name(), deserializedTopic.name());
      Assertions.assertEquals(topic.config().raw(), deserializedTopic.config().raw());
      Assertions.assertEquals(topic.internal(), topic.internal());
      Assertions.assertEquals(topic.topicPartitions(), deserializedTopic.topicPartitions());
    }
  }

  @Test
  void testReplicaSerializer() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      var replica = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join().replicas().get(0);
      var serializedReplica =
          Serializer.REPLICA.serialize(replica.topic(), Collections.emptyList(), replica);
      var buffer = ByteBuffer.wrap(serializedReplica);

      Assertions.assertEquals(replica.topic(), ByteUtils.readString(buffer, buffer.getShort()));
      Assertions.assertEquals(replica.partition(), buffer.getInt());
      buffer.get(new byte[buffer.getInt()]);
      Assertions.assertEquals(replica.lag(), buffer.getLong());
      Assertions.assertEquals(replica.size(), buffer.getLong());
      Assertions.assertEquals(replica.isLeader(), buffer.get() != 0);
      Assertions.assertEquals(replica.isSync(), buffer.get() != 0);
      Assertions.assertEquals(replica.isFuture(), buffer.get() != 0);
      Assertions.assertEquals(replica.isOffline(), buffer.get() != 0);
      Assertions.assertEquals(replica.isPreferredLeader(), buffer.get() != 0);
      Assertions.assertEquals(replica.path(), ByteUtils.readString(buffer, buffer.getShort()));
    }
  }

  @Test
  void testReplicaDeserializer() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      var replica = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join().replicas().get(0);
      var serializedReplica =
          Serializer.REPLICA.serialize(replica.topic(), Collections.emptyList(), replica);
      var deserializedReplica =
          Deserializer.REPLICA.deserialize(
              replica.topic(), Collections.emptyList(), serializedReplica);

      Assertions.assertEquals(replica, deserializedReplica);
    }
  }

  @Test
  void testClusterInfoSerializer() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      var clusterInfo = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join();
      var serializedClusterInfo =
          Serializer.CLUSTER_INFO.serialize("topic", Collections.emptyList(), clusterInfo);
      var buffer = ByteBuffer.wrap(serializedClusterInfo);

      Assertions.assertEquals(
          clusterInfo.clusterId(), ByteUtils.readString(buffer, buffer.getShort()));
      Assertions.assertEquals(clusterInfo.nodes().size(), buffer.getInt());
      IntStream.range(0, clusterInfo.nodes().size())
          .forEach(i -> buffer.get(new byte[buffer.getInt()]));
      Assertions.assertEquals(clusterInfo.topics().size(), buffer.getInt());
      IntStream.range(0, clusterInfo.topics().size())
          .forEach(i -> buffer.get(new byte[buffer.getInt()]));
      Assertions.assertEquals(clusterInfo.replicas().size(), buffer.getInt());
      IntStream.range(0, clusterInfo.replicas().size())
          .forEach(i -> buffer.get(new byte[buffer.getInt()]));
      Assertions.assertEquals(0, buffer.remaining());
    }
  }

  @Test
  void testClusterInfoDeserializer() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      var clusterInfo = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join();
      var serializedClusterInfo =
          Serializer.CLUSTER_INFO.serialize("topic", Collections.emptyList(), clusterInfo);
      var deserializedClusterInfo =
          Deserializer.CLUSTER_INFO.deserialize(
              "topic", Collections.emptyList(), serializedClusterInfo);

      Assertions.assertEquals(clusterInfo.clusterId(), deserializedClusterInfo.clusterId());
      Assertions.assertTrue(clusterInfo.nodes().containsAll(deserializedClusterInfo.nodes()));
      Assertions.assertEquals(
          clusterInfo.topics().keySet(), deserializedClusterInfo.topics().keySet());
      Assertions.assertTrue(deserializedClusterInfo.replicas().containsAll(clusterInfo.replicas()));
    }
  }

  @Test
  void testSerializeEmptyClusterInfo(){
    var clusterInfo = ClusterInfo.empty();
    var serializedInfo = Serializer.CLUSTER_INFO.serialize("topic", Collections.emptyList(), clusterInfo);
    var deserializedClusterInfo = Deserializer.CLUSTER_INFO.deserialize("topic", Collections.emptyList(), serializedInfo);

    Assertions.assertEquals(clusterInfo.clusterId(), deserializedClusterInfo.clusterId());
    Assertions.assertEquals(clusterInfo.nodes(), deserializedClusterInfo.nodes());
    Assertions.assertEquals(clusterInfo.topics(), deserializedClusterInfo.topics());
    Assertions.assertEquals(clusterInfo.replicas(), deserializedClusterInfo.replicas());
  }
}
