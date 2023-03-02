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
package org.astraea.common.assignor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.astraea.common.admin.NodeInfo;
import org.astraea.it.Service;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AssignorTest {
  private final Service SERVICE = Service.builder().numberOfBrokers(1).build();

  @Test
  void testSubscriptionConvert() {
    var data = "rack=1";
    var userData = ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
    var kafkaSubscription =
        new ConsumerPartitionAssignor.Subscription(List.of("test"), userData, null);
    var ourSubscription = Subscription.from(kafkaSubscription);

    Assertions.assertEquals(kafkaSubscription.topics(), ourSubscription.topics());
    Assertions.assertNull(kafkaSubscription.ownedPartitions());
    Assertions.assertEquals(0, ourSubscription.ownedPartitions().size());
    Assertions.assertEquals(kafkaSubscription.groupInstanceId(), ourSubscription.groupInstanceId());
    Assertions.assertEquals("1", ourSubscription.userData().get("rack"));
    Assertions.assertNull(ourSubscription.userData().get("rack=1"));
  }

  @Test
  void testGroupSubscriptionConvert() {
    var kafkaUser1Subscription =
        new ConsumerPartitionAssignor.Subscription(
            List.of("test1", "test2"), convert("rack=1"), null);
    var kafkaUser2Subscription =
        new ConsumerPartitionAssignor.Subscription(
            List.of("test1", "test2"),
            convert("rack=2"),
            List.of(new TopicPartition("test1", 0), new TopicPartition("test2", 1)));
    kafkaUser2Subscription.setGroupInstanceId(Optional.of("astraea"));
    var kafkaGroupSubscription =
        new ConsumerPartitionAssignor.GroupSubscription(
            Map.of("user1", kafkaUser1Subscription, "user2", kafkaUser2Subscription));
    var ourGroupSubscription = GroupSubscription.from(kafkaGroupSubscription);

    var ourUser1Subscription = ourGroupSubscription.groupSubscription().get("user1");
    var ourUser2Subscription = ourGroupSubscription.groupSubscription().get("user2");

    Assertions.assertEquals(Optional.empty(), ourUser1Subscription.groupInstanceId());
    Assertions.assertEquals(0, ourUser1Subscription.ownedPartitions().size());
    Assertions.assertEquals("1", ourUser1Subscription.userData().get("rack"));
    Assertions.assertEquals(List.of("test1", "test2"), ourUser1Subscription.topics());
    Assertions.assertEquals(
        "astraea",
        ourUser2Subscription.groupInstanceId().isPresent()
            ? ourUser2Subscription.groupInstanceId().get()
            : Optional.empty());
    Assertions.assertEquals(
        List.of(
            org.astraea.common.admin.TopicPartition.of("test1", 0),
            org.astraea.common.admin.TopicPartition.of("test2", 1)),
        ourUser2Subscription.ownedPartitions());
    Assertions.assertEquals("2", ourUser2Subscription.userData().get("rack"));
    Assertions.assertEquals(List.of("test1", "test2"), ourUser2Subscription.topics());
  }

  @Test
  void testJMXPort() {
    var randomAssignor = new RandomAssignor();
    randomAssignor.configure(Map.of());
    Assertions.assertEquals(Optional.empty(), randomAssignor.jmxPortGetter.apply(0));
    randomAssignor.configure(Map.of("broker.1000.jmx.port", "12345"));
    Assertions.assertEquals(Optional.of(12345), randomAssignor.jmxPortGetter.apply(1000));
    Assertions.assertNotEquals(Optional.of(12345), randomAssignor.jmxPortGetter.apply(0));

    var random2 = new RandomAssignor();
    random2.configure(Map.of("jmx.port", "8000", "broker.1002.jmx.port", "8888"));
    Assertions.assertEquals(Optional.of(8000), random2.jmxPortGetter.apply(0));
    Assertions.assertEquals(Optional.of(8000), random2.jmxPortGetter.apply(1));
    Assertions.assertEquals(Optional.of(8000), random2.jmxPortGetter.apply(2));
    Assertions.assertEquals(Optional.of(8888), random2.jmxPortGetter.apply(1002));
  }

  private static ByteBuffer convert(String value) {
    return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void testUnregisterId() {
    var assignor = new RandomAssignor();
    assignor.configure(Map.of("broker.1000.jmx.port", "8000", "broker.1001.jmx.port", "8100"));
    var nodes =
        List.of(
            NodeInfo.of(1000, "192.168.103.1", 8000),
            NodeInfo.of(1001, "192.168.103.2", 8100),
            NodeInfo.of(-1, "local jmx", 0));
    var unregister = assignor.checkUnregister(nodes);
    Assertions.assertEquals(2, unregister.size());
    Assertions.assertEquals("192.168.103.1", unregister.get(1000));
    Assertions.assertEquals("192.168.103.2", unregister.get(1001));
  }

  @Test
  void testAddNode() {
    var assignor = new RandomAssignor();
    // Get broker id
    var brokerId = SERVICE.dataFolders().keySet().stream().findAny().get();
    var jmxAddr = SERVICE.jmxServiceURL().getHost();
    var jmxPort = SERVICE.jmxServiceURL().getPort();
    assignor.configure(Map.of("broker." + brokerId + ".jmx.port", jmxPort));

    var nodes = new ArrayList<NodeInfo>();
    nodes.add(NodeInfo.of(brokerId, jmxAddr, jmxPort));
    var unregisterNode = assignor.checkUnregister(nodes);
    Assertions.assertEquals(1, unregisterNode.size());
    Assertions.assertEquals(jmxAddr, unregisterNode.get(brokerId));
    assignor.registerJMX(Map.of(brokerId, jmxAddr));
    unregisterNode = assignor.checkUnregister(nodes);
    Assertions.assertEquals(0, unregisterNode.size());

    // after add a new node, assignor check unregister node

    nodes.add(NodeInfo.of(1001, "192.168.103.2", 8000));
    unregisterNode = assignor.checkUnregister(nodes);
    Assertions.assertEquals(1, unregisterNode.size());
    Assertions.assertEquals("192.168.103.2", unregisterNode.get(1001));
  }
}
