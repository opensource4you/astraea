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
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Replica;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.ConnectorConfigs;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.connector.ConnectorMetrics;
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
  }

  @Test
  void testKeyDistribution() {
    testConfig(PerfSource.KEY_DISTRIBUTION_DEF.name(), "a");
  }

  @Test
  void testValueDistribution() {
    testConfig(PerfSource.VALUE_DISTRIBUTION_DEF.name(), "a");
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
  }
}
