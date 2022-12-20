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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.DistributionType;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.ConnectorConfigs;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.connector.ConnectorMetrics;
import org.astraea.connector.MetadataStorage;
import org.astraea.it.RequireSingleWorkerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerfSourceTest extends RequireSingleWorkerCluster {

  @Test
  void testDefaultConfig() {
    var name = Utils.randomString();
    var client = ConnectorClient.builder().url(workerUrl()).build();
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
    testConfig(PerfSource.KEY_LENGTH_DEF.name(), "a");
  }

  @Test
  void testValueLength() {
    testConfig(PerfSource.VALUE_LENGTH_DEF.name(), "a");
  }

  @Test
  void testFrequency() {
    testConfig(PerfSource.FREQUENCY_DEF.name(), "a");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PerfSource.FREQUENCY_DEF.validator().accept("aa", "bbb"));
    Assertions.assertDoesNotThrow(() -> PerfSource.FREQUENCY_DEF.validator().accept("aa", "10s"));
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

  @Test
  void testSpecifyPartition() {
    testConfig(PerfSource.SPECIFY_PARTITIONS_DEF.name(), "a");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PerfSource.SPECIFY_PARTITIONS_DEF.validator().accept("aa", "bbb"));
    Assertions.assertDoesNotThrow(
        () -> PerfSource.SPECIFY_PARTITIONS_DEF.validator().accept("a", "1,2,10"));

    var name = Utils.randomString();
    var topicName = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(10).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));
      var client = ConnectorClient.builder().url(workerUrl()).build();
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
                  PerfSource.SPECIFY_PARTITIONS_DEF.name(),
                  "0"))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(3));
      Assertions.assertNotEquals(
          0,
          admin
              .latestOffsets(Set.of(TopicPartition.of(topicName, 0)))
              .toCompletableFuture()
              .join()
              .get(TopicPartition.of(topicName, 0)));
      Assertions.assertEquals(
          0,
          admin
              .latestOffsets(Set.of(TopicPartition.of(topicName, 1)))
              .toCompletableFuture()
              .join()
              .get(TopicPartition.of(topicName, 1)));
    }
  }

  private void testConfig(String name, String errorValue) {
    var client = ConnectorClient.builder().url(workerUrl()).build();
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
    var client = ConnectorClient.builder().url(workerUrl()).build();
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
    try (var admin = Admin.of(bootstrapServers())) {
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
    var client = ConnectorClient.builder().url(workerUrl()).build();
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
        ConnectorMetrics.sourceTaskInfo(MBeanClient.local()).stream()
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
        ConnectorMetrics.taskError(MBeanClient.local()).stream()
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
        ConnectorMetrics.connectorInfo(MBeanClient.local()).stream()
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
  }

  @Test
  void testInit() {
    var task = new PerfSource.Task();
    task.init(Configuration.of(Map.of(ConnectorConfigs.TOPICS_KEY, "a")), MetadataStorage.EMPTY);
    Assertions.assertNotNull(task.rand);
    Assertions.assertNotNull(task.topics);
    Assertions.assertNotNull(task.frequency);
    Assertions.assertNotNull(task.keySelector);
    Assertions.assertNotNull(task.keySizeGenerator);
    Assertions.assertNotNull(task.keys);
    Assertions.assertNotNull(task.valueSelector);
    Assertions.assertNotNull(task.valueSizeGenerator);
    Assertions.assertNotNull(task.values);
  }

  @Test
  void testKeyAndValue() {
    var task = new PerfSource.Task();
    task.init(
        Configuration.of(
            Map.of(
                ConnectorConfigs.TOPICS_KEY,
                "a",
                PerfSource.KEY_DISTRIBUTION_DEF.name(),
                "uniform",
                PerfSource.VALUE_DISTRIBUTION_DEF.name(),
                "uniform")),
        MetadataStorage.EMPTY);
    var keys =
        IntStream.range(0, 100)
            .mapToObj(ignored -> Optional.ofNullable(task.key()))
            .flatMap(Optional::stream)
            .collect(Collectors.toSet());
    Assertions.assertEquals(keys.size(), task.keys.size());
    Assertions.assertNotEquals(0, keys.size());
    var values =
        IntStream.range(0, 100)
            .mapToObj(ignored -> Optional.ofNullable(task.value()))
            .flatMap(Optional::stream)
            .collect(Collectors.toSet());
    Assertions.assertEquals(values.size(), task.values.size());
    Assertions.assertNotEquals(0, values.size());
  }

  @Test
  void testZeroKeySize() {
    var task = new PerfSource.Task();
    task.init(
        Configuration.of(
            Map.of(ConnectorConfigs.TOPICS_KEY, "a", PerfSource.KEY_LENGTH_DEF.name(), "0Byte")),
        MetadataStorage.EMPTY);
    Assertions.assertNull(task.key());
    Assertions.assertEquals(0, task.keys.size());
  }

  @Test
  void testZeroValueSize() {
    var task = new PerfSource.Task();
    task.init(
        Configuration.of(
            Map.of(ConnectorConfigs.TOPICS_KEY, "a", PerfSource.VALUE_LENGTH_DEF.name(), "0Byte")),
        MetadataStorage.EMPTY);
    Assertions.assertNull(task.value());
    Assertions.assertEquals(0, task.values.size());
  }
}
