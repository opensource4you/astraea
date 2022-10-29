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
package org.astraea.common.metrics.client.admin;

import java.time.Duration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.client.HasNodeMetrics;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AdminMetricsTest extends RequireBrokerCluster {
  @Test
  void testMultiBrokers() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(3).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));
      var metrics = AdminMetrics.nodes(MBeanClient.local());
      Assertions.assertNotEquals(1, metrics.size());
      Assertions.assertTrue(
          metrics.stream().map(HasNodeMetrics::brokerId).anyMatch(id -> brokerIds().contains(id)));
      metrics.forEach(AdminMetricsTest::check);
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

  @Test
  void testMetrics() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(3).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));
      var metrics =
          AdminMetrics.of(MBeanClient.local()).stream()
              .filter(m -> m.clientId().equals(admin.clientId()))
              .findFirst()
              .get();
      Assertions.assertDoesNotThrow(metrics::connectionCloseRate);
      Assertions.assertDoesNotThrow(metrics::connectionCloseTotal);
      Assertions.assertDoesNotThrow(metrics::connectionCount);
      Assertions.assertDoesNotThrow(metrics::connectionCreationRate);
      Assertions.assertDoesNotThrow(metrics::connectionCreationTotal);
      Assertions.assertDoesNotThrow(metrics::failedAuthenticationRate);
      Assertions.assertDoesNotThrow(metrics::failedAuthenticationTotal);
      Assertions.assertDoesNotThrow(metrics::failedReauthenticationRate);
      Assertions.assertDoesNotThrow(metrics::failedReauthenticationTotal);
      Assertions.assertDoesNotThrow(metrics::incomingByteRate);
      Assertions.assertDoesNotThrow(metrics::incomingByteTotal);
      Assertions.assertDoesNotThrow(metrics::ioTimeNsAvg);
      Assertions.assertDoesNotThrow(metrics::ioTimeNsTotal);
      Assertions.assertDoesNotThrow(metrics::ioWaitTimeNsAvg);
      Assertions.assertDoesNotThrow(metrics::ioWaitTimeNsTotal);
      Assertions.assertDoesNotThrow(metrics::networkIoRate);
      Assertions.assertDoesNotThrow(metrics::networkIoTotal);
      Assertions.assertDoesNotThrow(metrics::outgoingByteRate);
      Assertions.assertDoesNotThrow(metrics::outgoingByteTotal);
      Assertions.assertDoesNotThrow(metrics::reauthenticationLatencyAvg);
      Assertions.assertDoesNotThrow(metrics::reauthenticationLatencyMax);
      Assertions.assertDoesNotThrow(metrics::requestRate);
      Assertions.assertDoesNotThrow(metrics::requestSizeAvg);
      Assertions.assertDoesNotThrow(metrics::requestSizeMax);
      Assertions.assertDoesNotThrow(metrics::requestTotal);
      Assertions.assertDoesNotThrow(metrics::responseRate);
      Assertions.assertDoesNotThrow(metrics::responseTotal);
      Assertions.assertDoesNotThrow(metrics::selectRate);
      Assertions.assertDoesNotThrow(metrics::selectTotal);
      Assertions.assertDoesNotThrow(metrics::successfulAuthenticationNoReauthTotal);
      Assertions.assertDoesNotThrow(metrics::successfulAuthenticationRate);
      Assertions.assertDoesNotThrow(metrics::successfulAuthenticationTotal);
      Assertions.assertDoesNotThrow(metrics::successfulReauthenticationRate);
      Assertions.assertDoesNotThrow(metrics::successfulReauthenticationTotal);
    }
  }
}
