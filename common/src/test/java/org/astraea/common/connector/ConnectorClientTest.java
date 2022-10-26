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
package org.astraea.common.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.astraea.common.Utils;
import org.astraea.common.connector.impl.TestTextSourceConnector;
import org.astraea.it.RequireSingleWorkerCluster;
import org.junit.jupiter.api.Test;

class ConnectorClientTest extends RequireSingleWorkerCluster {

  @Test
  void testInfo() throws ExecutionException, InterruptedException {
    var connectorClient = ConnectorClient.builder().build(workerUrl());
    var info = connectorClient.info().toCompletableFuture().get();
    assertFalse(Utils.isBlank(info.commit()));
    assertFalse(Utils.isBlank(info.version()));
    assertFalse(Utils.isBlank(info.kafkaClusterId()));
  }

  @Test
  void testConnectors() throws ExecutionException, InterruptedException {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().build(workerUrl());
    var connectors = connectorClient.connectors().toCompletableFuture().get();
    assertFalse(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    connectorClient
        .createConnector(connectorName, getExampleConnector())
        .toCompletableFuture()
        .get();
    connectors = connectorClient.connectors().toCompletableFuture().get();
    assertTrue(connectors.stream().anyMatch(x -> x.equals(connectorName)));
  }

  @Test
  void testConnectorByName() throws ExecutionException, InterruptedException {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().build(workerUrl());

    var executionException =
        assertThrows(
            ExecutionException.class,
            () -> connectorClient.connector(connectorName).toCompletableFuture().get());
    var exception = getWorkerException(executionException);
    assertEquals(404, exception.errorCode());
    assertTrue(exception.getMessage().contains(connectorName));

    connectorClient
        .createConnector(connectorName, getExampleConnector())
        .toCompletableFuture()
        .get();
    var connector = connectorClient.connector(connectorName).toCompletableFuture().get();
    assertEquals(connectorName, connector.name());
    assertExampleConnector(connector.config());
  }

  @Test
  void testCreateConnector() throws ExecutionException, InterruptedException {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().build(workerUrl());
    var exampleConnector = new HashMap<>(getExampleConnector());
    exampleConnector.put("tasks.max", "3");

    var createdConnectorInfo =
        connectorClient
            .createConnector(connectorName, exampleConnector)
            .toCompletableFuture()
            .get();
    assertEquals(connectorName, createdConnectorInfo.name());
    var config = createdConnectorInfo.config();
    assertEquals("3", config.get("tasks.max"));
    assertEquals("myTopic", config.get("topics"));
    assertEquals(TestTextSourceConnector.class.getName(), config.get("connector.class"));

    Utils.waitNoException(
        () -> {
          var connectorInfo = connectorClient.connector(connectorName).toCompletableFuture().get();
          assertEquals(3, connectorInfo.tasks().size());
          assertTrue(
              connectorInfo.tasks().stream().allMatch(x -> connectorName.equals(x.connector())));
          assertEquals(
              3,
              connectorInfo.tasks().stream()
                  .map(TaskInfo::task)
                  .filter(x -> x > -1)
                  .distinct()
                  .count());
        });
  }

  @Test
  void testUpdateConnector() throws ExecutionException, InterruptedException {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().build(workerUrl());
    var exampleConnector = getExampleConnector();

    var connector =
        connectorClient
            .createConnector(connectorName, exampleConnector)
            .toCompletableFuture()
            .get();
    assertEquals("1", connector.config().get("tasks.max"));
    assertEquals("myTopic", connector.config().get("topics"));
    var connectors = connectorClient.connectors().toCompletableFuture().get();
    assertTrue(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    var updateConfig = new HashMap<>(exampleConnector);
    updateConfig.put("tasks.max", "2");
    updateConfig.put("topics", "myTopic2");

    connector =
        connectorClient.updateConnector(connectorName, updateConfig).toCompletableFuture().get();
    assertEquals("2", connector.config().get("tasks.max"));
    assertEquals("myTopic2", connector.config().get("topics"));

    connector = connectorClient.connector(connectorName).toCompletableFuture().get();
    assertEquals("2", connector.config().get("tasks.max"));
    assertEquals("myTopic2", connector.config().get("topics"));
  }

  @Test
  void testDeleteConnector() throws ExecutionException, InterruptedException {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().build(workerUrl());
    var exampleConnector = getExampleConnector();

    connectorClient.createConnector(connectorName, exampleConnector).toCompletableFuture().get();
    var connectors = connectorClient.connectors().toCompletableFuture().get();
    assertTrue(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    connectorClient.deleteConnector(connectorName).toCompletableFuture().get();
    connectors = connectorClient.connectors().toCompletableFuture().get();
    assertFalse(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    var executionException =
        assertThrows(
            ExecutionException.class,
            () -> connectorClient.deleteConnector("unknown").toCompletableFuture().get());

    var exception = getWorkerException(executionException);
    assertEquals(404, exception.errorCode());
  }

  private WorkerResponseException getWorkerException(ExecutionException executionException) {
    executionException.printStackTrace();
    assertEquals(WorkerResponseException.class, executionException.getCause().getClass());
    return (WorkerResponseException) executionException.getCause();
  }

  private void assertExampleConnector(Map<String, String> config) {
    assertEquals("1", config.get("tasks.max"));
    assertEquals("myTopic", config.get("topics"));
    assertEquals(TestTextSourceConnector.class.getName(), config.get("connector.class"));
  }

  private Map<String, String> getExampleConnector() {
    return Map.of(
        "connector.class",
        TestTextSourceConnector.class.getName(),
        "tasks.max",
        "1",
        "topics",
        "myTopic");
  }
}
