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
package org.astraea.common.consumer.assignor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.astraea.common.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConsumerPartitionAssignorTest {
  @Test
  void testSubscriptionConvert() {
    var data = "rack=1";
    var userData = ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
    var kafkaSubscription =
        new ConsumerPartitionAssignor.Subscription(List.of("test"), userData, null);
    var ourSubscription = org.astraea.common.consumer.assignor.Subscription.from(kafkaSubscription);

    Assertions.assertEquals(kafkaSubscription.topics(), ourSubscription.topics());
    Assertions.assertEquals(kafkaSubscription.ownedPartitions(), ourSubscription.ownedPartitions());
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
    var ourGroupSubscription =
        org.astraea.common.consumer.assignor.GroupSubscription.from(kafkaGroupSubscription);

    var ourUser1Subscription = ourGroupSubscription.groupSubscription().get("user1");
    var ourUser2Subscription = ourGroupSubscription.groupSubscription().get("user2");

    Assertions.assertEquals(Optional.empty(), ourUser1Subscription.groupInstanceId());
    Assertions.assertEquals(null, ourUser1Subscription.ownedPartitions());
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
    randomAssignor.configure(Configuration.of(Map.of()));
    Assertions.assertEquals(Optional.empty(), randomAssignor.jmxPortGetter.apply(0));
    randomAssignor.configure(Configuration.of(Map.of("broker.1000.jmx.port", "12345")));
    Assertions.assertEquals(Optional.of(12345), randomAssignor.jmxPortGetter.apply(1000));
    Assertions.assertNotEquals(Optional.of(12345), randomAssignor.jmxPortGetter.apply(0));

    var random2 = new RandomAssignor();
    random2.configure(Configuration.of(Map.of("jmx.port", "8000", "broker.1002.jmx.port", "8888")));
    Assertions.assertEquals(Optional.of(8000), random2.jmxPortGetter.apply(0));
    Assertions.assertEquals(Optional.of(8000), random2.jmxPortGetter.apply(1));
    Assertions.assertEquals(Optional.of(8000), random2.jmxPortGetter.apply(2));
    Assertions.assertEquals(Optional.of(8888), random2.jmxPortGetter.apply(1002));
  }

  private static ByteBuffer convert(String value) {
    return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
  }
}
