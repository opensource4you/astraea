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
import org.astraea.it.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HasConsumerFetchMetricsTest extends RequireSingleBrokerCluster {

  @Test
  void testClientId() {
    var topic = Utils.randomString(10);

    try (var producer = Producer.of(bootstrapServers())) {
      IntStream.range(0, 10)
          .forEach(
              index ->
                  producer
                      .sender()
                      .topic(topic)
                      .key(String.valueOf(index).getBytes(StandardCharsets.UTF_8))
                      .run());
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
      var metrics = ConsumerMetrics.fetches(MBeanClient.local());
      Assertions.assertEquals(1, metrics.size());
      var m = metrics.iterator().next();
      Assertions.assertNotNull(m.clientId());
      Assertions.assertDoesNotThrow(m::bytesConsumedRate);
      Assertions.assertDoesNotThrow(m::bytesConsumedTotal);
      Assertions.assertDoesNotThrow(m::fetchLatencyAvg);
      Assertions.assertDoesNotThrow(m::fetchLatencyMax);
      Assertions.assertDoesNotThrow(m::fetchRate);
      Assertions.assertDoesNotThrow(m::fetchSizeAvg);
      Assertions.assertDoesNotThrow(m::fetchSizeMax);
      Assertions.assertDoesNotThrow(m::fetchThrottleTimeAvg);
      Assertions.assertDoesNotThrow(m::fetchThrottleTimeMax);
      Assertions.assertDoesNotThrow(m::fetchTotal);
      Assertions.assertDoesNotThrow(m::recordsConsumedRate);
      Assertions.assertDoesNotThrow(m::recordsConsumedTotal);
      Assertions.assertDoesNotThrow(m::recordsLagMax);
      Assertions.assertDoesNotThrow(m::recordsLeadMin);
      Assertions.assertDoesNotThrow(m::recordsPerRequestAvg);
    }
  }
}
