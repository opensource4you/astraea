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
package org.astraea.app.metrics.client.producer;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.MBeanClient;
import org.astraea.app.metrics.client.HasNodeMetrics;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProducerMetricsTest extends RequireBrokerCluster {

  @Test
  void testMultiBrokers() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(3).create();
      Utils.sleep(Duration.ofSeconds(3));
      producer
          .sender()
          .topic(topic)
          .value(new byte[10])
          .partition(0)
          .run()
          .toCompletableFuture()
          .get();
      producer
          .sender()
          .topic(topic)
          .value(new byte[10])
          .partition(1)
          .run()
          .toCompletableFuture()
          .get();
      producer
          .sender()
          .topic(topic)
          .value(new byte[10])
          .partition(2)
          .run()
          .toCompletableFuture()
          .get();
      var metrics = ProducerMetrics.nodes(MBeanClient.local());
      Assertions.assertNotEquals(1, metrics.size());
      Assertions.assertTrue(
          metrics.stream()
              .map(HasNodeMetrics::brokerId)
              .collect(Collectors.toUnmodifiableList())
              .containsAll(brokerIds()));
      metrics.forEach(ProducerMetricsTest::check);
    }
  }

  private static void check(HasNodeMetrics metrics) {
    Assertions.assertNotEquals(0D, metrics.incomingByteRate());
    Assertions.assertNotEquals(0D, metrics.incomingByteTotal());
    Assertions.assertNotEquals(0D, metrics.outgoingByteRate());
    Assertions.assertNotEquals(0D, metrics.outgoingByteTotal());
    Assertions.assertNotEquals(0D, metrics.requestLatencyAvg());
    Assertions.assertNotEquals(0D, metrics.requestLatencyMax());
    Assertions.assertNotEquals(0D, metrics.requestRate());
    Assertions.assertNotEquals(0D, metrics.requestSizeAvg());
    Assertions.assertNotEquals(0D, metrics.requestSizeMax());
    Assertions.assertNotEquals(0D, metrics.requestTotal());
    Assertions.assertNotEquals(0D, metrics.responseRate());
    Assertions.assertNotEquals(0D, metrics.responseTotal());
  }
}
