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
package org.astraea.common.metrics.client.producer;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.client.HasNodeMetrics;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProducerMetricsTest extends RequireBrokerCluster {

  @Test
  void testMetrics() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    try (var producer = Producer.of(bootstrapServers())) {
      producer.sender().topic(topic).run().toCompletableFuture().get();
      var metrics =
          ProducerMetrics.of(MBeanClient.local()).stream()
              .filter(m -> m.clientId().equals(producer.clientId()))
              .findFirst()
              .get();
      Assertions.assertDoesNotThrow(metrics::batchSizeAvg);
      Assertions.assertDoesNotThrow(metrics::batchSizeMax);
      Assertions.assertDoesNotThrow(metrics::batchSplitRate);
      Assertions.assertDoesNotThrow(metrics::batchSplitTotal);
      Assertions.assertDoesNotThrow(metrics::bufferAvailableBytes);
      Assertions.assertDoesNotThrow(metrics::bufferExhaustedRate);
      Assertions.assertDoesNotThrow(metrics::bufferExhaustedTotal);
      Assertions.assertDoesNotThrow(metrics::bufferTotalBytes);
      Assertions.assertDoesNotThrow(metrics::bufferPoolWaitRatio);
      Assertions.assertDoesNotThrow(metrics::bufferPoolWaitTimeNsTotal);
      Assertions.assertDoesNotThrow(metrics::compressionRateAvg);
      Assertions.assertDoesNotThrow(metrics::connectionCloseRate);
      Assertions.assertDoesNotThrow(metrics::connectionCloseTotal);
      Assertions.assertDoesNotThrow(metrics::connectionCount);
      Assertions.assertDoesNotThrow(metrics::connectionCreationRate);
      Assertions.assertDoesNotThrow(metrics::connectionCreationTotal);
      Assertions.assertDoesNotThrow(metrics::failedAuthenticationRate);
      Assertions.assertDoesNotThrow(metrics::failedAuthenticationTotal);
      Assertions.assertDoesNotThrow(metrics::failedReauthenticationRate);
      Assertions.assertDoesNotThrow(metrics::failedReauthenticationTotal);
      Assertions.assertDoesNotThrow(metrics::flushTimeNsTotal);
      Assertions.assertDoesNotThrow(metrics::incomingByteRate);
      Assertions.assertDoesNotThrow(metrics::incomingByteTotal);
      Assertions.assertDoesNotThrow(metrics::ioTimeNsAvg);
      Assertions.assertDoesNotThrow(metrics::ioTimeNsTotal);
      Assertions.assertDoesNotThrow(metrics::ioWaitTimeNsAvg);
      Assertions.assertDoesNotThrow(metrics::ioWaitTimeNsTotal);
      Assertions.assertDoesNotThrow(metrics::metadataAge);
      Assertions.assertDoesNotThrow(metrics::networkIoRate);
      Assertions.assertDoesNotThrow(metrics::networkIoTotal);
      Assertions.assertDoesNotThrow(metrics::outgoingByteRate);
      Assertions.assertDoesNotThrow(metrics::outgoingByteTotal);
      Assertions.assertDoesNotThrow(metrics::produceThrottleTimeAvg);
      Assertions.assertDoesNotThrow(metrics::produceThrottleTimeMax);
      Assertions.assertDoesNotThrow(metrics::reauthenticationLatencyAvg);
      Assertions.assertDoesNotThrow(metrics::reauthenticationLatencyMax);
      Assertions.assertDoesNotThrow(metrics::recordErrorRate);
      Assertions.assertDoesNotThrow(metrics::recordErrorTotal);
      Assertions.assertDoesNotThrow(metrics::recordQueueTimeAvg);
      Assertions.assertDoesNotThrow(metrics::recordQueueTimeMax);
      Assertions.assertDoesNotThrow(metrics::recordRetryRate);
      Assertions.assertDoesNotThrow(metrics::recordRetryTotal);
      Assertions.assertDoesNotThrow(metrics::recordSendRate);
      Assertions.assertDoesNotThrow(metrics::recordSendTotal);
      Assertions.assertDoesNotThrow(metrics::recordSizeAvg);
      Assertions.assertDoesNotThrow(metrics::recordSizeMax);
      Assertions.assertDoesNotThrow(metrics::recordsPerRequestAvg);
      Assertions.assertDoesNotThrow(metrics::requestLatencyAvg);
      Assertions.assertDoesNotThrow(metrics::requestLatencyMax);
      Assertions.assertDoesNotThrow(metrics::requestRate);
      Assertions.assertDoesNotThrow(metrics::requestSizeAvg);
      Assertions.assertDoesNotThrow(metrics::requestSizeMax);
      Assertions.assertDoesNotThrow(metrics::requestTotal);
      Assertions.assertDoesNotThrow(metrics::requestInFlight);
      Assertions.assertDoesNotThrow(metrics::responseRate);
      Assertions.assertDoesNotThrow(metrics::responseTotal);
      Assertions.assertDoesNotThrow(metrics::selectRate);
      Assertions.assertDoesNotThrow(metrics::selectTotal);
      Assertions.assertDoesNotThrow(metrics::successfulAuthenticationNoReauthTotal);
      Assertions.assertDoesNotThrow(metrics::successfulAuthenticationRate);
      Assertions.assertDoesNotThrow(metrics::successfulAuthenticationTotal);
      Assertions.assertDoesNotThrow(metrics::successfulReauthenticationRate);
      Assertions.assertDoesNotThrow(metrics::successfulReauthenticationTotal);
      Assertions.assertDoesNotThrow(metrics::txnAbortTimeNsTotal);
      Assertions.assertDoesNotThrow(metrics::txnBeginTimeNsTotal);
      Assertions.assertDoesNotThrow(metrics::txnCommitTimeNsTotal);
      Assertions.assertDoesNotThrow(metrics::txnInitTimeNsTotal);
      Assertions.assertDoesNotThrow(metrics::txnSendOffsetsTimeNsTotal);
      Assertions.assertDoesNotThrow(metrics::waitingThreads);
    }
  }

  @Test
  void testTopicMetrics() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    try (var producer = Producer.of(bootstrapServers())) {
      producer.sender().topic(topic).run().toCompletableFuture().get();
      var metrics = ProducerMetrics.topics(MBeanClient.local());
      Assertions.assertNotEquals(0, metrics.stream().filter(m -> m.topic().equals(topic)).count());
      var producerTopicMetrics =
          metrics.stream().filter(m -> m.clientId().equals(producer.clientId())).findFirst().get();
      Assertions.assertNotEquals(0D, producerTopicMetrics.byteRate());
      Assertions.assertNotEquals(0D, producerTopicMetrics.byteTotal());
      Assertions.assertEquals(1D, producerTopicMetrics.compressionRate());
      Assertions.assertDoesNotThrow(producerTopicMetrics::recordErrorRate);
      Assertions.assertDoesNotThrow(producerTopicMetrics::recordErrorTotal);
      Assertions.assertDoesNotThrow(producerTopicMetrics::recordRetryRate);
      Assertions.assertDoesNotThrow(producerTopicMetrics::recordRetryTotal);
      Assertions.assertNotEquals(0D, producerTopicMetrics.recordSendRate());
      Assertions.assertNotEquals(0D, producerTopicMetrics.recordSendTotal());
    }
  }

  @Test
  void testNodeMetrics() throws ExecutionException, InterruptedException {
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
