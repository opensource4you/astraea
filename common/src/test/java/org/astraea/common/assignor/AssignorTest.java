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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.astraea.common.consumer.ConsumerConfigs;
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
    var ourSubscription = SubscriptionInfo.from(kafkaSubscription);

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
    var ourGroupSubscription = GroupSubscriptionInfo.from(kafkaGroupSubscription);

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
    randomAssignor.configure(
        Map.of(ConsumerConfigs.BOOTSTRAP_SERVERS_CONFIG, SERVICE.bootstrapServers()));
    Assertions.assertThrows(
        NoSuchElementException.class, () -> randomAssignor.jmxPortGetter.apply(0));
    randomAssignor.configure(
        Map.of(
            "jmx.port",
            String.valueOf(SERVICE.jmxServiceURL().getPort()),
            "broker.1000.jmx.port",
            "12345",
            ConsumerConfigs.BOOTSTRAP_SERVERS_CONFIG,
            SERVICE.bootstrapServers()));
    Assertions.assertEquals(12345, randomAssignor.jmxPortGetter.apply(1000));

    var random2 = new RandomAssignor();
    random2.configure(
        Map.of(
            "jmx.port",
            String.valueOf(SERVICE.jmxServiceURL().getPort()),
            "broker.1002.jmx.port",
            "8888",
            ConsumerConfigs.BOOTSTRAP_SERVERS_CONFIG,
            SERVICE.bootstrapServers()));
    Assertions.assertEquals(SERVICE.jmxServiceURL().getPort(), random2.jmxPortGetter.apply(0));
    Assertions.assertEquals(SERVICE.jmxServiceURL().getPort(), random2.jmxPortGetter.apply(1));
    Assertions.assertEquals(SERVICE.jmxServiceURL().getPort(), random2.jmxPortGetter.apply(2));
    Assertions.assertEquals(8888, random2.jmxPortGetter.apply(1002));
  }

  private static ByteBuffer convert(String value) {
    return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
  }
}
