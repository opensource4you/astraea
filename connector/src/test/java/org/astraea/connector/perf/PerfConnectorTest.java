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
import org.astraea.it.RequireSingleWorkerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerfConnectorTest extends RequireSingleWorkerCluster {

  @Test
  void testDefaultConfig() {
    var client = ConnectorClient.builder().url(workerUrl()).build();
    var validation =
        client
            .validate(
                PerfConnector.class.getSimpleName(),
                Map.of(
                    ConnectorClient.NAME_KEY,
                    Utils.randomString(),
                    ConnectorClient.CONNECTOR_CLASS_KEY,
                    PerfConnector.class.getName(),
                    ConnectorClient.TASK_MAX_KEY,
                    "1",
                    ConnectorClient.TOPICS_KEY,
                    "abc"))
            .toCompletableFuture()
            .join();
    Assertions.assertEquals(
        0,
        validation.errorCount(),
        validation.configs().stream()
            .flatMap(c -> c.value().errors().stream())
            .collect(Collectors.joining(",")));
  }

  @Test
  void testKeyLength() {
    var client = ConnectorClient.builder().url(workerUrl()).build();
    var validation =
        client
            .validate(
                PerfConnector.class.getSimpleName(),
                Map.of(
                    ConnectorClient.NAME_KEY,
                    Utils.randomString(),
                    ConnectorClient.CONNECTOR_CLASS_KEY,
                    PerfConnector.class.getName(),
                    ConnectorClient.TASK_MAX_KEY,
                    "1",
                    ConnectorClient.TOPICS_KEY,
                    "abc",
                    PerfConnector.KEY_LENGTH_DEF.name(),
                    "a"))
            .toCompletableFuture()
            .join();
    Assertions.assertEquals(1, validation.errorCount());
    Assertions.assertNotEquals(
        0,
        validation.configs().stream()
            .filter(c -> c.definition().name().equals(PerfConnector.KEY_LENGTH_DEF.name()))
            .findFirst()
            .get()
            .value()
            .errors()
            .size());
  }

  @Test
  void testValueLength() {
    var client = ConnectorClient.builder().url(workerUrl()).build();
    var validation =
        client
            .validate(
                PerfConnector.class.getSimpleName(),
                Map.of(
                    ConnectorClient.NAME_KEY,
                    Utils.randomString(),
                    ConnectorClient.CONNECTOR_CLASS_KEY,
                    PerfConnector.class.getName(),
                    ConnectorClient.TASK_MAX_KEY,
                    "1",
                    ConnectorClient.TOPICS_KEY,
                    "abc",
                    PerfConnector.VALUE_LENGTH_DEF.name(),
                    "a"))
            .toCompletableFuture()
            .join();
    Assertions.assertEquals(1, validation.errorCount());
    Assertions.assertNotEquals(
        0,
        validation.configs().stream()
            .filter(c -> c.definition().name().equals(PerfConnector.VALUE_LENGTH_DEF.name()))
            .findFirst()
            .get()
            .value()
            .errors()
            .size());
  }

  @Test
  void testFrequency() {
    var client = ConnectorClient.builder().url(workerUrl()).build();
    var validation =
        client
            .validate(
                PerfConnector.class.getSimpleName(),
                Map.of(
                    ConnectorClient.NAME_KEY,
                    Utils.randomString(),
                    ConnectorClient.CONNECTOR_CLASS_KEY,
                    PerfConnector.class.getName(),
                    ConnectorClient.TASK_MAX_KEY,
                    "1",
                    ConnectorClient.TOPICS_KEY,
                    "abc",
                    PerfConnector.FREQUENCY_DEF.name(),
                    "a"))
            .toCompletableFuture()
            .join();
    Assertions.assertEquals(1, validation.errorCount());
    Assertions.assertNotEquals(
        0,
        validation.configs().stream()
            .filter(c -> c.definition().name().equals(PerfConnector.FREQUENCY_DEF.name()))
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
                ConnectorClient.CONNECTOR_CLASS_KEY,
                PerfConnector.class.getName(),
                ConnectorClient.TASK_MAX_KEY,
                "1",
                ConnectorClient.TOPICS_KEY,
                topicName))
        .toCompletableFuture()
        .join();
    Utils.sleep(Duration.ofSeconds(3));

    var status = client.connectorStatus(name).toCompletableFuture().join();
    Assertions.assertNotEquals(0, status.tasks().size());
    status
        .tasks()
        .forEach(t -> Assertions.assertEquals("RUNNING", t.state(), t.trace().toString()));

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
}
