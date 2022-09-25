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
import org.astraea.common.Utils;
import org.astraea.common.connector.impl.TestTextSourceConnector;
import org.astraea.it.RequireSingleWorkerCluster;
import org.junit.jupiter.api.Test;

class ConnectorClientTest extends RequireSingleWorkerCluster {

  @Test
  void testInfo() {
    var connectorClient = ConnectorClient.builder().build(workerUrl());
    var info = connectorClient.info();
    assertFalse(Utils.isEmpty(info.commit()));
    assertFalse(Utils.isEmpty(info.version()));
    // TODO: 2022-10-03 clusterId is null
  }

  @Test
  void testConnectors() {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().build(workerUrl());
    var connectors = connectorClient.connectors();
    assertFalse(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    connectorClient.createConnector(connectorName, getExampleConnector());
    connectors = connectorClient.connectors();
    assertTrue(connectors.stream().anyMatch(x -> x.equals(connectorName)));
  }

  @Test
  void testConnectorByName() {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().build(workerUrl());
    var exception =
        assertThrows(WorkerResponseException.class, () -> connectorClient.connector(connectorName));

    assertEquals(404, exception.errorCode());
    assertTrue(exception.message().contains(connectorName));

    connectorClient.createConnector(connectorName, getExampleConnector());
    var connector = connectorClient.connector(connectorName);
    assertEquals(connectorName, connector.name());
    assertExampleConnector(connector.config());
  }

  @Test
  void testCreateConnector() {
    // TODO: 2022-10-03 test tasks
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().build(workerUrl());
    var exampleConnector = getExampleConnector();

    var connector = connectorClient.createConnector(connectorName, exampleConnector);
    assertEquals(connectorName, connector.name());
    assertExampleConnector(connector.config());
  }

  @Test
  void testUpdateConnector() {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().build(workerUrl());
    var exampleConnector = getExampleConnector();

    var connector = connectorClient.createConnector(connectorName, exampleConnector);
    assertEquals("1", connector.config().get("tasks.max"));
    assertEquals("myTopic", connector.config().get("topics"));
    var connectors = connectorClient.connectors();
    assertTrue(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    var updateConfig = new HashMap<>(exampleConnector);
    updateConfig.put("tasks.max", "2");
    updateConfig.put("topics", "myTopic2");

    connector = connectorClient.updateConnector(connectorName, updateConfig);
    assertEquals("2", connector.config().get("tasks.max"));
    assertEquals("myTopic2", connector.config().get("topics"));

    connector = connectorClient.connector(connectorName);
    assertEquals("2", connector.config().get("tasks.max"));
    assertEquals("myTopic2", connector.config().get("topics"));
  }

  @Test
  void testDeleteConnector() {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().build(workerUrl());
    var exampleConnector = getExampleConnector();

    connectorClient.createConnector(connectorName, exampleConnector);
    var connectors = connectorClient.connectors();
    assertTrue(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    connectorClient.deleteConnector(connectorName);
    connectors = connectorClient.connectors();
    assertFalse(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    var workerResponseException =
        assertThrows(
            WorkerResponseException.class, () -> connectorClient.deleteConnector("unknown"));

    assertEquals(404, workerResponseException.errorCode());
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
