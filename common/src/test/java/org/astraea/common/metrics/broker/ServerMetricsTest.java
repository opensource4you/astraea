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
package org.astraea.common.metrics.broker;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.MetricsTestUtils;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class ServerMetricsTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(1).build();

  @BeforeAll
  static void createBroker() {
    // call broker-related method to initialize broker cluster
    Assertions.assertNotEquals(0, SERVICE.dataFolders().size());
  }

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testAppInfo() {
    ServerMetrics.appInfo(MBeanClient.local()).forEach(MetricsTestUtils::validate);
  }

  @ParameterizedTest()
  @EnumSource(value = ServerMetrics.DelayedOperationPurgatory.class)
  void testPurgatorySize(ServerMetrics.DelayedOperationPurgatory request) {
    var m = request.fetch(MBeanClient.local());
    Assertions.assertDoesNotThrow(m::value);
    MetricsTestUtils.validate(m);
  }

  @ParameterizedTest()
  @EnumSource(value = ServerMetrics.KafkaServer.class)
  void testKafkaServer(ServerMetrics.KafkaServer request) {
    MetricsTestUtils.validate(request.fetch(MBeanClient.local()));
  }

  @Test
  void testKafkaServerOtherMetrics() {
    MetricsTestUtils.validate(ServerMetrics.KafkaServer.CLUSTER_ID.fetch(MBeanClient.local()));
  }

  @Test
  void testSocketMetrics() {
    var socketMetric = ServerMetrics.Socket.socket(MBeanClient.local());

    assertDoesNotThrow(socketMetric::brokerConnectionAcceptRate);
    assertDoesNotThrow(socketMetric::memoryPoolAvgDepletedPercent);
    assertDoesNotThrow(socketMetric::memoryPoolDepletedTimeTotal);
  }

  @Test
  void testSocketListenerMetrics() {
    var socketListenerMetrics = ServerMetrics.Socket.socketListener(MBeanClient.local());
    assertTrue(socketListenerMetrics.size() > 0);
    socketListenerMetrics.forEach(
        x -> {
          assertNotNull(x.listener());

          assertDoesNotThrow(x::connectionAcceptRate);
          assertDoesNotThrow(x::connectionAcceptThrottleTime);
          assertDoesNotThrow(x::ipConnectionAcceptThrottleTime);
        });
  }

  @Test
  void testSocketNetworkProcessorMetrics() {
    var socketNetworkProcessorMetrics =
        ServerMetrics.Socket.socketNetworkProcessor(MBeanClient.local());
    assertTrue(socketNetworkProcessorMetrics.size() > 0);
    socketNetworkProcessorMetrics.forEach(
        x -> {
          assertNotNull(x.listener());
          assertNotNull(x.networkProcessor());

          assertDoesNotThrow(x::connectionCloseRate);
          assertDoesNotThrow(x::incomingByteTotal);
          assertDoesNotThrow(x::selectTotal);
          assertDoesNotThrow(x::successfulAuthenticationRate);
          assertDoesNotThrow(x::reauthenticationLatencyAvg);
          assertDoesNotThrow(x::networkIoRate);
          assertDoesNotThrow(x::connectionCreationTotal);
          assertDoesNotThrow(x::successfulReauthenticationRate);
          assertDoesNotThrow(x::requestSizeMax);
          assertDoesNotThrow(x::connectionCloseRate);
          assertDoesNotThrow(x::successfulAuthenticationTotal);
          assertDoesNotThrow(x::ioTimeNsTotal);
          assertDoesNotThrow(x::connectionCount);
          assertDoesNotThrow(x::failedReauthenticationTotal);
          assertDoesNotThrow(x::requestRate);
          assertDoesNotThrow(x::successfulReauthenticationTotal);
          assertDoesNotThrow(x::responseRate);
          assertDoesNotThrow(x::connectionCreationRate);
          assertDoesNotThrow(x::ioWaitTimeNsAvg);
          assertDoesNotThrow(x::ioWaitTimeNsTotal);
          assertDoesNotThrow(x::outgoingByteRate);
          assertDoesNotThrow(x::iotimeTotal);
          assertDoesNotThrow(x::ioRatio);
          assertDoesNotThrow(x::requestSizeAvg);
          assertDoesNotThrow(x::outgoingByteTotal);
          assertDoesNotThrow(x::expiredConnectionsKilledCount);
          assertDoesNotThrow(x::connectionCloseTotal);
          assertDoesNotThrow(x::failedReauthenticationRate);
          assertDoesNotThrow(x::networkIoTotal);
          assertDoesNotThrow(x::failedAuthenticationTotal);
          assertDoesNotThrow(x::incomingByteRate);
          assertDoesNotThrow(x::selectRate);
          assertDoesNotThrow(x::ioTimeNsAvg);
          assertDoesNotThrow(x::reauthenticationLatencyMax);
          assertDoesNotThrow(x::responseTotal);
          assertDoesNotThrow(x::failedAuthenticationRate);
          assertDoesNotThrow(x::ioWaitRatio);
          assertDoesNotThrow(x::successfulAuthenticationNoReauthTotal);
          assertDoesNotThrow(x::requestTotal);
          assertDoesNotThrow(x::ioWaittimeTotal);
        });
  }

  @Test
  void testSocketClientMetrics() {
    var clientMetrics = ServerMetrics.Socket.client(MBeanClient.local());
    assertTrue(clientMetrics.size() > 0);
    clientMetrics.forEach(
        x -> {
          assertNotNull(x.listener());
          assertNotNull(x.networkProcessor());
          assertNotNull(x.clientSoftwareName());
          assertNotNull(x.clientSoftwareVersion());

          assertDoesNotThrow(x::connections);
        });
  }

  @Test
  void testKafkaMetricsOf() {
    Arrays.stream(ServerMetrics.BrokerTopic.values())
        .forEach(
            t ->
                Assertions.assertEquals(
                    t, ServerMetrics.BrokerTopic.ofAlias(t.metricName().toLowerCase(Locale.ROOT))));
    Arrays.stream(ServerMetrics.BrokerTopic.values())
        .forEach(
            t ->
                Assertions.assertEquals(
                    t, ServerMetrics.BrokerTopic.ofAlias(t.metricName().toUpperCase(Locale.ROOT))));
    assertThrows(
        IllegalArgumentException.class, () -> ServerMetrics.BrokerTopic.ofAlias("nothing"));
  }

  @ParameterizedTest
  @EnumSource(ServerMetrics.BrokerTopic.class)
  void testBrokerTopic(ServerMetrics.BrokerTopic brokerTopic) {
    var object =
        new ServerMetrics.BrokerTopic.Meter(
            new BeanObject("object", Map.of("name", brokerTopic.metricName()), Map.of()));
    Assertions.assertEquals(1, brokerTopic.of(List.of(object)).size());

    Assertions.assertEquals(
        0,
        brokerTopic
            .of(
                List.of(
                    new ServerMetrics.BrokerTopic.Meter(
                        new BeanObject(
                            "object", Map.of("name", Utils.randomString(10)), Map.of()))))
            .size());
  }

  @ParameterizedTest
  @EnumSource(ServerMetrics.Topic.class)
  void testTopic(ServerMetrics.Topic topic) {
    var name = Utils.randomString();
    try (var producer = Producer.of(SERVICE.bootstrapServers())) {
      producer
          .send(Record.builder().topic(name).key(new byte[10]).build())
          .toCompletableFuture()
          .join();
    }
    try (var consumer =
        Consumer.forTopics(Set.of(name))
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .bootstrapServers(SERVICE.bootstrapServers())
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(5));
      Assertions.assertEquals(1, records.size());
    }
    var meters = topic.fetch(MBeanClient.local());
    Assertions.assertNotEquals(0, meters.size());
    Assertions.assertNotEquals(0, meters.stream().filter(m -> m.topic().equals(name)).count());
    meters.forEach(
        m -> {
          Assertions.assertNotNull(m.metricsName());
          Assertions.assertNotNull(m.topic());
          Assertions.assertNotNull(m.type());
          MetricsTestUtils.validate(m);
        });
  }

  @Test
  void testAllEnumNameUnique() {
    Assertions.assertTrue(
        MetricsTestUtils.metricDistinct(
            ServerMetrics.ReplicaManager.values(), ServerMetrics.ReplicaManager::metricName));
    Assertions.assertTrue(
        MetricsTestUtils.metricDistinct(
            ServerMetrics.DelayedOperationPurgatory.values(),
            ServerMetrics.DelayedOperationPurgatory::metricName));
    Assertions.assertTrue(
        MetricsTestUtils.metricDistinct(
            ServerMetrics.BrokerTopic.values(), ServerMetrics.BrokerTopic::metricName));
  }
}
