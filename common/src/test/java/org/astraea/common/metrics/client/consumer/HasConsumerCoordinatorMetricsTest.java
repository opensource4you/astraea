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
package org.astraea.common.metrics.client.consumer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HasConsumerCoordinatorMetricsTest extends RequireSingleBrokerCluster {

  @Test
  void testClientId() {
    var topic = Utils.randomString(10);

    try (var producer = Producer.of(bootstrapServers())) {
      IntStream.range(0, 10)
          .forEach(
              index ->
                  producer.send(
                      Record.builder()
                          .topic(topic)
                          .key(String.valueOf(index).getBytes(StandardCharsets.UTF_8))
                          .build()));
    }
    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .build()) {
      Assertions.assertEquals(10, consumer.poll(10, Duration.ofSeconds(5)).size());
      consumer.commitOffsets(Duration.ofSeconds(2));
      var metrics = ConsumerMetrics.coordinators(MBeanClient.local());
      Assertions.assertEquals(1, metrics.size());
      var m = metrics.iterator().next();
      Assertions.assertNotNull(m.clientId());
      Assertions.assertEquals(1D, m.assignedPartitions());
      Assertions.assertDoesNotThrow(m::commitLatencyAvg);
      Assertions.assertDoesNotThrow(m::commitLatencyMax);
      Assertions.assertDoesNotThrow(m::commitRate);
      Assertions.assertDoesNotThrow(m::commitRate);
      Assertions.assertDoesNotThrow(m::commitRate);
      Assertions.assertDoesNotThrow(m::commitTotal);
      Assertions.assertDoesNotThrow(m::failedRebalanceRatePerHour);
      Assertions.assertDoesNotThrow(m::failedRebalanceTotal);
      Assertions.assertDoesNotThrow(m::heartbeatRate);
      Assertions.assertDoesNotThrow(m::joinRate);
      Assertions.assertDoesNotThrow(m::joinTimeAvg);
      Assertions.assertDoesNotThrow(m::joinTimeMax);
      Assertions.assertDoesNotThrow(m::joinTotal);
      Assertions.assertDoesNotThrow(m::lastHeartbeatSecondsAgo);
      Assertions.assertDoesNotThrow(m::lastRebalanceSecondsAgo);
      Assertions.assertDoesNotThrow(m::partitionAssignedLatencyAvg);
      Assertions.assertDoesNotThrow(m::partitionAssignedLatencyMax);
      // no partitions are revoked so no metrics
      Assertions.assertTrue(Double.isNaN(m.partitionLostLatencyAvg()));
      Assertions.assertTrue(Double.isNaN(m.partitionLostLatencyMax()));
      Assertions.assertTrue(Double.isNaN(m.partitionRevokedLatencyAvg()));
      Assertions.assertTrue(Double.isNaN(m.partitionRevokedLatencyMax()));
      Assertions.assertDoesNotThrow(m::rebalanceLatencyAvg);
      Assertions.assertDoesNotThrow(m::rebalanceLatencyMax);
      Assertions.assertDoesNotThrow(m::rebalanceLatencyTotal);
      Assertions.assertDoesNotThrow(m::rebalanceRatePerHour);
      Assertions.assertDoesNotThrow(m::rebalanceTotal);
      Assertions.assertDoesNotThrow(m::syncRate);
      Assertions.assertDoesNotThrow(m::syncTimeAvg);
      Assertions.assertDoesNotThrow(m::syncTimeMax);
      Assertions.assertDoesNotThrow(m::syncTotal);
    }
  }
}
