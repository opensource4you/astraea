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
package org.astraea.app.metrics.client.consumer;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.common.Utils;
import org.astraea.app.consumer.Consumer;
import org.astraea.app.metrics.MBeanClient;
import org.astraea.app.metrics.client.HasNodeMetrics;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConsumerMetricsTest extends RequireBrokerCluster {
  @Test
  void testSingleBroker() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var consumer =
            Consumer.forTopics(Set.of(topic)).bootstrapServers(bootstrapServers()).build()) {
      admin.creator().topic(topic).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(3));
      var owner =
          admin.replicas(Set.of(topic)).get(TopicPartition.of(topic, 0)).get(0).nodeInfo().id();
      consumer.poll(Duration.ofSeconds(5));
      var metrics = ConsumerMetrics.node(MBeanClient.local(), owner);
      Assertions.assertEquals(1, metrics.size());
      check(metrics.values().stream().findAny().get());
    }
  }

  @Test
  void testMultiBrokers() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var consumer =
            Consumer.forTopics(Set.of(topic)).bootstrapServers(bootstrapServers()).build()) {
      admin.creator().topic(topic).numberOfPartitions(3).create();
      Utils.sleep(Duration.ofSeconds(3));
      consumer.poll(Duration.ofSeconds(5));
      var metrics = ConsumerMetrics.nodes(MBeanClient.local());
      Assertions.assertNotEquals(1, metrics.size());
      Assertions.assertTrue(
          metrics.stream()
              .map(HasNodeMetrics::brokerId)
              .collect(Collectors.toUnmodifiableList())
              .containsAll(brokerIds()));
      metrics.forEach(ConsumerMetricsTest::check);
    }
  }

  private static void check(HasNodeMetrics metrics) {
    Assertions.assertNotEquals(0D, metrics.incomingByteRate());
    Assertions.assertNotEquals(0D, metrics.incomingByteTotal());
    Assertions.assertNotEquals(0D, metrics.outgoingByteRate());
    Assertions.assertNotEquals(0D, metrics.outgoingByteTotal());
    Assertions.assertEquals(Double.NaN, metrics.requestLatencyAvg());
    Assertions.assertEquals(Double.NaN, metrics.requestLatencyMax());
    Assertions.assertNotEquals(0D, metrics.requestRate());
    Assertions.assertNotEquals(0D, metrics.requestSizeAvg());
    Assertions.assertNotEquals(0D, metrics.requestSizeMax());
    Assertions.assertNotEquals(0D, metrics.requestTotal());
    Assertions.assertNotEquals(0D, metrics.responseRate());
    Assertions.assertNotEquals(0D, metrics.responseTotal());
  }
}
