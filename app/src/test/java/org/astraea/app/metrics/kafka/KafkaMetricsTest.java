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
package org.astraea.app.metrics.kafka;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.admin.Admin;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.java.JvmMemory;
import org.astraea.app.metrics.java.OperatingSystemInfo;
import org.astraea.app.metrics.jmx.MBeanClient;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class KafkaMetricsTest extends RequireBrokerCluster {

  private MBeanServer mBeanServer;
  private JMXConnectorServer jmxServer;
  private MBeanClient mBeanClient;

  @BeforeEach
  void setUp() throws IOException {
    JMXServiceURL serviceURL = new JMXServiceURL("service:jmx:rmi://127.0.0.1");

    mBeanServer = ManagementFactory.getPlatformMBeanServer();

    jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(serviceURL, null, mBeanServer);
    jmxServer.start();

    mBeanClient = MBeanClient.of(jmxServer.getAddress());
  }

  @AfterEach
  void tearDown() throws Exception {
    jmxServer.stop();
    mBeanServer = null;
    mBeanClient.close();
  }

  @Test
  void testAllEnumNameUnique() {
    // arrange act
    Set<String> collectedName =
        Arrays.stream(KafkaMetrics.BrokerTopic.values())
            .map(KafkaMetrics.BrokerTopic::metricName)
            .collect(Collectors.toSet());

    // assert
    assertEquals(KafkaMetrics.BrokerTopic.values().length, collectedName.size());
  }

  @ParameterizedTest
  @EnumSource(value = KafkaMetrics.BrokerTopic.class)
  void testRequestBrokerTopicMetrics(KafkaMetrics.BrokerTopic metric) {
    // act
    BrokerTopicMetricsResult result = metric.fetch(mBeanClient);

    // assert access attribute will not throw casting error
    // assert attribute actually exists
    assertDoesNotThrow(result::count);
    assertDoesNotThrow(result::eventType);
    assertDoesNotThrow(result::fifteenMinuteRate);
    assertDoesNotThrow(result::fiveMinuteRate);
    assertDoesNotThrow(result::meanRate);
    assertDoesNotThrow(result::oneMinuteRate);
    assertDoesNotThrow(result::rateUnit);
  }

  @ParameterizedTest()
  @EnumSource(value = KafkaMetrics.Purgatory.class)
  void testPurgatorySize(KafkaMetrics.Purgatory request) {
    // act assert type casting correct and field exists
    assertDoesNotThrow(() -> request.size(mBeanClient));
  }

  @ParameterizedTest()
  @EnumSource(value = KafkaMetrics.Request.class)
  void testRequestTotalTimeMs(KafkaMetrics.Request request) {
    // act
    TotalTimeMs totalTimeMs = request.totalTimeMs(mBeanClient);

    // assert type casting correct and field exists
    assertDoesNotThrow(totalTimeMs::percentile50);
    assertDoesNotThrow(totalTimeMs::percentile75);
    assertDoesNotThrow(totalTimeMs::percentile95);
    assertDoesNotThrow(totalTimeMs::percentile98);
    assertDoesNotThrow(totalTimeMs::percentile99);
    assertDoesNotThrow(totalTimeMs::percentile999);
    assertDoesNotThrow(totalTimeMs::count);
    assertDoesNotThrow(totalTimeMs::max);
    assertDoesNotThrow(totalTimeMs::mean);
    assertDoesNotThrow(totalTimeMs::min);
    assertDoesNotThrow(totalTimeMs::stdDev);
  }

  @ParameterizedTest()
  @EnumSource(value = KafkaMetrics.TopicPartition.class)
  void testTopicPartitionMetrics(KafkaMetrics.TopicPartition request) throws InterruptedException {
    try (var admin = Admin.of(bootstrapServers())) {
      // there are only 3 brokers, so 10 partitions can make each broker has some partitions
      admin.creator().topic(Utils.randomString(5)).numberOfPartitions(10).create();
    }

    // wait for topic creation
    TimeUnit.SECONDS.sleep(2);

    var beans = request.fetch(mBeanClient);
    assertNotEquals(0, beans.size());
  }

  @Test
  void testGlobalPartitionCount() {
    // act
    assertDoesNotThrow(() -> KafkaMetrics.TopicPartition.globalPartitionCount(mBeanClient));
  }

  @Test
  void testUnderReplicatedPartitions() {
    assertDoesNotThrow(() -> KafkaMetrics.TopicPartition.underReplicatedPartitions(mBeanClient));
  }

  @Test
  void testSize() {
    // arrange
    try (Admin admin = Admin.of(bootstrapServers())) {
      String topicName = getClass().getName();
      admin.creator().topic(topicName).numberOfPartitions(10).create();

      // act assert
      assertDoesNotThrow(() -> KafkaMetrics.TopicPartition.size(mBeanClient, topicName));
    }
  }

  @Test
  void testKafkaMetricsOf() {
    assertEquals(
        KafkaMetrics.BrokerTopic.BytesInPerSec, KafkaMetrics.BrokerTopic.of("ByTeSiNpErSeC"));
    assertEquals(
        KafkaMetrics.BrokerTopic.BytesOutPerSec, KafkaMetrics.BrokerTopic.of("bytesoutpersec"));
    assertEquals(
        KafkaMetrics.BrokerTopic.MessagesInPerSec, KafkaMetrics.BrokerTopic.of("MessagesInPERSEC"));
    assertThrows(IllegalArgumentException.class, () -> KafkaMetrics.BrokerTopic.of("nothing"));
  }

  @Test
  void operatingSystem() {
    OperatingSystemInfo operatingSystemInfo = KafkaMetrics.Host.operatingSystem(mBeanClient);
    assertDoesNotThrow(operatingSystemInfo::arch);
    assertDoesNotThrow(operatingSystemInfo::availableProcessors);
    assertDoesNotThrow(operatingSystemInfo::committedVirtualMemorySize);
    assertDoesNotThrow(operatingSystemInfo::freePhysicalMemorySize);
    assertDoesNotThrow(operatingSystemInfo::freeSwapSpaceSize);
    assertDoesNotThrow(operatingSystemInfo::name);
    assertDoesNotThrow(operatingSystemInfo::processCpuLoad);
    assertDoesNotThrow(operatingSystemInfo::processCpuTime);
    assertDoesNotThrow(operatingSystemInfo::systemCpuLoad);
    assertDoesNotThrow(operatingSystemInfo::systemLoadAverage);
    assertDoesNotThrow(operatingSystemInfo::totalPhysicalMemorySize);
    assertDoesNotThrow(operatingSystemInfo::totalSwapSpaceSize);
    assertDoesNotThrow(operatingSystemInfo::version);
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void maxFileDescriptorCount() {
    OperatingSystemInfo operatingSystemInfo = KafkaMetrics.Host.operatingSystem(mBeanClient);
    assertDoesNotThrow(operatingSystemInfo::maxFileDescriptorCount);
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void openFileDescriptorCount() {
    OperatingSystemInfo operatingSystemInfo = KafkaMetrics.Host.operatingSystem(mBeanClient);
    assertDoesNotThrow(operatingSystemInfo::openFileDescriptorCount);
  }

  @Test
  void testJvmMemory() {
    JvmMemory jvmMemory = KafkaMetrics.Host.jvmMemory(mBeanClient);

    assertDoesNotThrow(() -> jvmMemory.heapMemoryUsage().getCommitted());
    assertDoesNotThrow(() -> jvmMemory.heapMemoryUsage().getMax());
    assertDoesNotThrow(() -> jvmMemory.heapMemoryUsage().getUsed());
    assertDoesNotThrow(() -> jvmMemory.heapMemoryUsage().getInit());
    assertDoesNotThrow(() -> jvmMemory.nonHeapMemoryUsage().getCommitted());
    assertDoesNotThrow(() -> jvmMemory.nonHeapMemoryUsage().getMax());
    assertDoesNotThrow(() -> jvmMemory.nonHeapMemoryUsage().getUsed());
    assertDoesNotThrow(() -> jvmMemory.nonHeapMemoryUsage().getInit());
  }

  @Test
  @EnabledOnOs(LINUX)
  void linuxDiskReadBytes() {
    assertDoesNotThrow(() -> KafkaMetrics.BrokerTopic.linuxDiskReadBytes(mBeanClient));
  }

  @Test
  @EnabledOnOs(LINUX)
  void linuxDiskWriteBytes() {
    assertDoesNotThrow(() -> KafkaMetrics.BrokerTopic.linuxDiskWriteBytes(mBeanClient));
  }
}
