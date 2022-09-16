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
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HasConsumerMetricsTest extends RequireSingleBrokerCluster {

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
            .fromBeginning()
            .build()) {
      Assertions.assertEquals(10, consumer.poll(10, Duration.ofSeconds(5)).size());
      consumer.commitOffsets(Duration.ofSeconds(2));
      var metrics = ConsumerMetrics.of(MBeanClient.local());
      Assertions.assertEquals(1, metrics.size());
      var m = metrics.iterator().next();
      Assertions.assertNotNull(m.clientId());
      Assertions.assertDoesNotThrow(m::reauthenticationLatencyAvg);
      Assertions.assertDoesNotThrow(m::ioTimeNsTotal);
      Assertions.assertDoesNotThrow(m::successfulAuthenticationTotal);
      Assertions.assertDoesNotThrow(m::ioWaittimeTotal);
      Assertions.assertDoesNotThrow(m::committedTimeNsTotal);
      Assertions.assertDoesNotThrow(m::reauthenticationLatencyMax);
      Assertions.assertDoesNotThrow(m::successfulAuthenticationRate);
      Assertions.assertDoesNotThrow(m::commitSyncTimeNsTotal);
      Assertions.assertDoesNotThrow(m::failedAuthenticationTotal);
      Assertions.assertDoesNotThrow(m::timeBetweenPollAvg);
      Assertions.assertDoesNotThrow(m::connectionCount);
      Assertions.assertDoesNotThrow(m::responseTotal);
      Assertions.assertDoesNotThrow(m::requestRate);
      Assertions.assertDoesNotThrow(m::incomingByteRate);
      Assertions.assertDoesNotThrow(m::lastPollSecondsAgo);
      Assertions.assertDoesNotThrow(m::successfulAuthenticationNoReauthTotal);
      Assertions.assertDoesNotThrow(m::timeBetweenPollMax);
      Assertions.assertDoesNotThrow(m::failedReauthenticationTotal);
      Assertions.assertDoesNotThrow(m::selectRate);
      Assertions.assertDoesNotThrow(m::successfulReauthenticationTotal);
      Assertions.assertDoesNotThrow(m::requestTotal);
      Assertions.assertDoesNotThrow(m::ioTimeNsAvg);
      Assertions.assertDoesNotThrow(m::iotimeTotal);
      Assertions.assertDoesNotThrow(m::ioWaitTimeNsTotal);
      Assertions.assertDoesNotThrow(m::ioWaitRatio);
      Assertions.assertDoesNotThrow(m::networkIoRate);
      Assertions.assertDoesNotThrow(m::connectionCreationRate);
      Assertions.assertDoesNotThrow(m::requestSizeAvg);
      Assertions.assertDoesNotThrow(m::ioRatio);
      Assertions.assertDoesNotThrow(m::responseRate);
      Assertions.assertDoesNotThrow(m::successfulReauthenticationRate);
      Assertions.assertDoesNotThrow(m::selectTotal);
      Assertions.assertDoesNotThrow(m::outgoingByteTotal);
      Assertions.assertDoesNotThrow(m::pollIdleRatioAvg);
      Assertions.assertDoesNotThrow(m::connectionCloseRate);
      Assertions.assertDoesNotThrow(m::incomingByteTotal);
      Assertions.assertDoesNotThrow(m::requestSizeMax);
      Assertions.assertDoesNotThrow(m::failedAuthenticationRate);
      Assertions.assertDoesNotThrow(m::outgoingByteRate);
      Assertions.assertDoesNotThrow(m::failedReauthenticationRate);
      Assertions.assertDoesNotThrow(m::ioWaitTimeNsAvg);
      Assertions.assertDoesNotThrow(m::connectionCloseTotal);
      Assertions.assertDoesNotThrow(m::connectionCreationTotal);
      Assertions.assertDoesNotThrow(m::networkIoTotal);
    }
  }
}
