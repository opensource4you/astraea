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
package org.astraea.connector.perf;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.DistributionType;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.ConnectorConfigs;
import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.connector.ConnectorMetrics;
import org.astraea.connector.MetadataStorage;
import org.astraea.connector.SourceConnector;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerfSourceTest {

  private static final Service SERVICE =
      Service.builder().numberOfWorkers(1).numberOfBrokers(1).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testDistributeConfigs() {
    var s = new PerfSource();
    var config = new Configuration(Map.of(SourceConnector.TOPICS_KEY, "a,b,c,d"));
    s.init(config, MetadataStorage.EMPTY);
    var configs = s.takeConfiguration(10);
    Assertions.assertEquals(4, configs.size());
    Assertions.assertEquals("a", configs.get(0).requireString(SourceConnector.TOPICS_KEY));
    Assertions.assertEquals("b", configs.get(1).requireString(SourceConnector.TOPICS_KEY));
    Assertions.assertEquals("c", configs.get(2).requireString(SourceConnector.TOPICS_KEY));
    Assertions.assertEquals("d", configs.get(3).requireString(SourceConnector.TOPICS_KEY));
  }

  @Test
  void testDefaultConfig() {
    var name = Utils.randomString();
    var client = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    client
        .createConnector(
            name,
            Map.of(
                ConnectorConfigs.CONNECTOR_CLASS_KEY,
                PerfSource.class.getName(),
                ConnectorConfigs.TASK_MAX_KEY,
                "1",
                ConnectorConfigs.TOPICS_KEY,
                "abc"))
        .toCompletableFuture()
        .join();

    Utils.sleep(Duration.ofSeconds(4));

    var status = client.connectorStatus(name).toCompletableFuture().join();
    Assertions.assertEquals("RUNNING", status.state());
    Assertions.assertEquals(1, status.tasks().size());
    status.tasks().forEach(t -> Assertions.assertEquals("RUNNING", t.state()));
  }

  @Test
  void testKeyLength() {
    testConfig(PerfSource.KEY_SIZE_DEF.name(), "a");
  }

  @Test
  void testValueLength() {
    testConfig(PerfSource.VALUE_SIZE_DEF.name(), "a");
  }

  @Test
  void testThroughput() {
    testConfig(PerfSource.THROUGHPUT_DEF.name(), "a");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PerfSource.THROUGHPUT_DEF.validator().accept("aa", "bbb"));
    Assertions.assertDoesNotThrow(() -> PerfSource.THROUGHPUT_DEF.validator().accept("aa", "10Kb"));

    var name = Utils.randomString();
    var topicName = Utils.randomString();
    var client = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    client
        .createConnector(
            name,
            Map.of(
                ConnectorConfigs.CONNECTOR_CLASS_KEY,
                PerfSource.class.getName(),
                ConnectorConfigs.TASK_MAX_KEY,
                "1",
                ConnectorConfigs.TOPICS_KEY,
                topicName,
                PerfSource.THROUGHPUT_DEF.name(),
                "1Byte"))
        .toCompletableFuture()
        .join();

    Utils.sleep(Duration.ofSeconds(3));

    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      var offsets =
          admin.latestOffsets(Set.of(TopicPartition.of(topicName, 0))).toCompletableFuture().join();
      Assertions.assertEquals(1, offsets.size());
      Assertions.assertEquals(1, offsets.get(TopicPartition.of(topicName, 0)));
    }
  }

  @Test
  void testKeyDistribution() {
    testConfig(PerfSource.KEY_DISTRIBUTION_DEF.name(), "a");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PerfSource.KEY_DISTRIBUTION_DEF.validator().accept("aa", "bbb"));
    for (var d : DistributionType.values())
      Assertions.assertDoesNotThrow(
          () -> PerfSource.KEY_DISTRIBUTION_DEF.validator().accept("a", d.alias()));
  }

  @Test
  void testValueDistribution() {
    testConfig(PerfSource.VALUE_DISTRIBUTION_DEF.name(), "a");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PerfSource.VALUE_DISTRIBUTION_DEF.validator().accept("aa", "bbb"));
    for (var d : DistributionType.values())
      Assertions.assertDoesNotThrow(
          () -> PerfSource.VALUE_DISTRIBUTION_DEF.validator().accept("a", d.alias()));
  }

  private void testConfig(String name, String errorValue) {
    var client = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    var validation =
        client
            .validate(
                PerfSource.class.getSimpleName(),
                Map.of(
                    ConnectorConfigs.NAME_KEY,
                    Utils.randomString(),
                    ConnectorConfigs.CONNECTOR_CLASS_KEY,
                    PerfSource.class.getName(),
                    ConnectorConfigs.TASK_MAX_KEY,
                    "1",
                    ConnectorConfigs.TOPICS_KEY,
                    "abc",
                    name,
                    errorValue))
            .toCompletableFuture()
            .join();
    Assertions.assertEquals(1, validation.errorCount());
    Assertions.assertNotEquals(
        0,
        validation.configs().stream()
            .filter(c -> c.definition().name().equals(name))
            .findFirst()
            .get()
            .value()
            .errors()
            .size());
  }

  @Test
  void testCreatePerf() {
    var name = Utils.randomString();
    var topicName = Utils.randomString();
    var client = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    client
        .createConnector(
            name,
            Map.of(
                ConnectorConfigs.CONNECTOR_CLASS_KEY,
                PerfSource.class.getName(),
                ConnectorConfigs.TASK_MAX_KEY,
                "1",
                ConnectorConfigs.TOPICS_KEY,
                topicName))
        .toCompletableFuture()
        .join();
    Utils.sleep(Duration.ofSeconds(3));

    var status = client.connectorStatus(name).toCompletableFuture().join();
    Assertions.assertNotEquals(0, status.tasks().size());
    status
        .tasks()
        .forEach(t -> Assertions.assertEquals("RUNNING", t.state(), t.error().toString()));

    // make sure there are some data
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      Assertions.assertTrue(
          admin.topicNames(false).toCompletableFuture().join().contains(topicName));
      Assertions.assertNotEquals(
          0,
          admin.clusterInfo(Set.of(topicName)).toCompletableFuture().join().replicas().stream()
              .mapToLong(Replica::size)
              .sum());
    }
  }

  @Test
  void testMetrics() {
    var name = Utils.randomString();
    var topicName = Utils.randomString();
    var client = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    client
        .createConnector(
            name,
            Map.of(
                ConnectorConfigs.CONNECTOR_CLASS_KEY,
                PerfSource.class.getName(),
                ConnectorConfigs.TASK_MAX_KEY,
                "1",
                ConnectorConfigs.TOPICS_KEY,
                topicName))
        .toCompletableFuture()
        .join();
    Utils.sleep(Duration.ofSeconds(3));

    var m0 =
        ConnectorMetrics.sourceTaskInfo(JndiClient.local()).stream()
            .filter(m -> m.connectorName().equals(name))
            .collect(Collectors.toList());
    Assertions.assertNotEquals(0, m0.size());
    m0.forEach(
        m -> {
          Assertions.assertNotNull(m.connectorName());
          Assertions.assertDoesNotThrow(m::taskId);
          Assertions.assertNotNull(m.taskType());
          Assertions.assertDoesNotThrow(m::pollBatchAvgTimeMs);
          Assertions.assertDoesNotThrow(m::pollBatchMaxTimeMs);
          Assertions.assertDoesNotThrow(m::sourceRecordActiveCountMax);
          Assertions.assertDoesNotThrow(m::sourceRecordActiveCountAvg);
          Assertions.assertDoesNotThrow(m::sourceRecordActiveCount);
          Assertions.assertDoesNotThrow(m::sourceRecordWriteRate);
          Assertions.assertDoesNotThrow(m::sourceRecordPollTotal);
          Assertions.assertDoesNotThrow(m::sourceRecordPollRate);
          Assertions.assertDoesNotThrow(m::sourceRecordWriteTotal);
        });

    var m1 =
        ConnectorMetrics.taskError(JndiClient.local()).stream()
            .filter(m -> m.connectorName().equals(name))
            .collect(Collectors.toList());
    Assertions.assertNotEquals(0, m1.size());
    m1.forEach(
        m -> {
          Assertions.assertEquals(0, m.lastErrorTimestamp());
          Assertions.assertEquals(0D, m.deadletterqueueProduceFailures());
          Assertions.assertEquals(0D, m.deadletterqueueProduceRequests());
          Assertions.assertEquals(0D, m.totalErrorsLogged());
          Assertions.assertEquals(0D, m.totalRecordErrors());
          Assertions.assertEquals(0D, m.totalRetries());
          Assertions.assertEquals(0D, m.totalRecordFailures());
          Assertions.assertEquals(0D, m.totalRecordsSkipped());
        });

    var m2 =
        ConnectorMetrics.connectorTaskInfo(JndiClient.local()).stream()
            .filter(m -> m.connectorName().equals(name))
            .collect(Collectors.toList());
    Assertions.assertEquals(1, m2.size());
    m2.forEach(
        m -> {
          Assertions.assertNotNull(m.connectorName());
          Assertions.assertDoesNotThrow(m::taskId);
          Assertions.assertNotNull(m.connectorType());
          Assertions.assertDoesNotThrow(m::batchSizeAvg);
          Assertions.assertDoesNotThrow(m::batchSizeMax);
          Assertions.assertDoesNotThrow(m::offsetCommitSuccessPercentage);
          Assertions.assertDoesNotThrow(m::offsetCommitFailurePercentage);
          Assertions.assertDoesNotThrow(m::offsetCommitAvgTimeMs);
          Assertions.assertDoesNotThrow(m::offsetCommitMaxTimeMs);
          Assertions.assertDoesNotThrow(m::pauseRatio);
          Assertions.assertDoesNotThrow(m::status);
        });

    var m3 =
        ConnectorMetrics.workerConnectorInfo(JndiClient.local()).stream()
            .filter(m -> m.connectorName().equals(name))
            .collect(Collectors.toList());
    Assertions.assertEquals(1, m3.size());
    m3.forEach(
        m -> {
          Assertions.assertDoesNotThrow(m::connectorDestroyedTaskCount);
          Assertions.assertDoesNotThrow(m::connectorFailedTaskCount);
          Assertions.assertDoesNotThrow(m::connectorPausedTaskCount);
          Assertions.assertDoesNotThrow(m::connectorRestartingTaskCount);
          Assertions.assertDoesNotThrow(m::connectorRunningTaskCount);
          Assertions.assertDoesNotThrow(m::connectorTotalTaskCount);
          Assertions.assertDoesNotThrow(m::connectorUnassignedTaskCount);
        });

    var m4 =
        ConnectorMetrics.connectorInfo(JndiClient.local()).stream()
            .filter(m -> m.connectorName().equals(name))
            .collect(Collectors.toList());
    Assertions.assertEquals(1, m4.size());
    m4.forEach(
        m -> {
          Assertions.assertEquals(PerfSource.class.getName(), m.connectorClass());
          Assertions.assertEquals("source", m.connectorType());
          Assertions.assertDoesNotThrow(m::connectorVersion);
          Assertions.assertDoesNotThrow(m::status);
        });
  }

  @Test
  void testInit() {
    var task = new PerfSource.Task();
    task.init(new Configuration(Map.of(ConnectorConfigs.TOPICS_KEY, "a")), MetadataStorage.EMPTY);
    Assertions.assertNotNull(task.recordGenerator);
    Assertions.assertEquals(1, task.specifyPartitions.size());
  }

  @Test
  void testKeyAndValue() {
    var task = new PerfSource.Task();
    task.init(
        new Configuration(
            Map.of(
                ConnectorConfigs.TOPICS_KEY,
                "a",
                PerfSource.KEY_DISTRIBUTION_DEF.name(),
                "uniform",
                PerfSource.VALUE_DISTRIBUTION_DEF.name(),
                "uniform")),
        MetadataStorage.EMPTY);
    var records = task.take();
    var keySizes =
        records.stream().map(r -> r.key().length).collect(Collectors.toUnmodifiableSet());
    Assertions.assertEquals(1, keySizes.size());
    var valueSizes =
        records.stream().map(r -> r.value().length).collect(Collectors.toUnmodifiableSet());
    Assertions.assertEquals(1, valueSizes.size());
  }

  @Test
  void testZeroKeySize() {
    var task = new PerfSource.Task();
    task.init(
        new Configuration(
            Map.of(ConnectorConfigs.TOPICS_KEY, "a", PerfSource.KEY_SIZE_DEF.name(), "0Byte")),
        MetadataStorage.EMPTY);
    var records = task.take();
    Assertions.assertNotEquals(0, records.size());
    records.forEach(r -> Assertions.assertNull(r.key()));
  }

  @Test
  void testZeroValueSize() {
    var task = new PerfSource.Task();
    task.init(
        new Configuration(
            Map.of(ConnectorConfigs.TOPICS_KEY, "a", PerfSource.VALUE_SIZE_DEF.name(), "0Byte")),
        MetadataStorage.EMPTY);
    var records = task.take();
    Assertions.assertNotEquals(0, records.size());
    records.forEach(r -> Assertions.assertNull(r.value()));
  }
}
