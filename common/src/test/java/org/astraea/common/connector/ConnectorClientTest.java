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

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.connector.impl.TestErrorSourceConnector;
import org.astraea.common.connector.impl.TestTextSourceConnector;
import org.astraea.common.http.HttpRequestException;
import org.astraea.it.RequireWorkerCluster;
import org.junit.jupiter.api.Test;

class ConnectorClientTest extends RequireWorkerCluster {

  @Test
  void testInfo() throws ExecutionException, InterruptedException {
    var connectorClient = ConnectorClient.builder().url(workerUrl()).build();
    var info = connectorClient.info().toCompletableFuture().get();
    assertFalse(isBlank(info.commit()));
    assertFalse(isBlank(info.version()));
    assertFalse(isBlank(info.kafkaClusterId()));
  }

  @Test
  void testConnectors() throws ExecutionException, InterruptedException {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().url(workerUrl()).build();
    var connectors = connectorClient.connectorNames().toCompletableFuture().get();
    assertFalse(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    connectorClient
        .createConnector(connectorName, getExampleConnector())
        .toCompletableFuture()
        .get();
    connectors = connectorClient.connectorNames().toCompletableFuture().get();
    assertTrue(connectors.stream().anyMatch(x -> x.equals(connectorName)));
  }

  @Test
  void testConnectorByName() throws ExecutionException, InterruptedException {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().url(workerUrl()).build();

    var executionException =
        assertThrows(
            ExecutionException.class,
            () -> connectorClient.connector(connectorName).toCompletableFuture().get());
    var exception = getResponseException(executionException);
    assertEquals(404, exception.code());
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
    var connectorClient = ConnectorClient.builder().url(workerUrl()).build();
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

    assertTrue(
        connectorClient
            .waitConnectorInfo(connectorName, x -> x.tasks().size() > 0, Duration.ofSeconds(10))
            .toCompletableFuture()
            .get());
    var connectorInfo = connectorClient.connector(connectorName).toCompletableFuture().get();
    assertEquals(3, connectorInfo.tasks().size());
    assertTrue(
        connectorInfo.tasks().stream().allMatch(x -> connectorName.equals(x.connectorName())));
    assertEquals(3, connectorInfo.tasks().stream().map(TaskInfo::taskId).distinct().count());
  }

  @Test
  void testUpdateConnector() throws ExecutionException, InterruptedException {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().url(workerUrl()).build();
    var exampleConnector = getExampleConnector();

    var connector =
        connectorClient
            .createConnector(connectorName, exampleConnector)
            .toCompletableFuture()
            .get();
    assertEquals("1", connector.config().get("tasks.max"));
    assertEquals("myTopic", connector.config().get("topics"));
    var connectors = connectorClient.connectorNames().toCompletableFuture().get();
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
    var connectorClient = ConnectorClient.builder().url(workerUrl()).build();
    var exampleConnector = getExampleConnector();

    connectorClient.createConnector(connectorName, exampleConnector).toCompletableFuture().get();
    var connectors = connectorClient.connectorNames().toCompletableFuture().get();
    assertTrue(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    connectorClient.deleteConnector(connectorName).toCompletableFuture().get();
    connectors = connectorClient.connectorNames().toCompletableFuture().get();
    assertFalse(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    var executionException =
        assertThrows(
            ExecutionException.class,
            () -> connectorClient.deleteConnector("unknown").toCompletableFuture().get());

    var exception = getResponseException(executionException);

    // In distribution mode, Request forward to another node will throw 500, otherwise 404.
    assertTrue(exception.getMessage().contains("unknown not found"));
  }

  @Test
  void testPlugin() throws ExecutionException, InterruptedException {
    var connectorClient = ConnectorClient.builder().url(workerUrl()).build();
    var plugins = connectorClient.plugins().toCompletableFuture().get();
    assertTrue(
        plugins.stream()
            .anyMatch(x -> TestTextSourceConnector.class.getName().equals(x.className())));
  }

  @Test
  void testUrls() throws ExecutionException, InterruptedException {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().urls(new HashSet<>(workerUrls())).build();
    connectorClient
        .createConnector(connectorName, getExampleConnector())
        .toCompletableFuture()
        .get();
    IntStream.range(0, 15)
        .forEach(
            x ->
                Utils.packException(
                    () -> {
                      var connector =
                          connectorClient.connector(connectorName).toCompletableFuture().get();
                      assertEquals(connectorName, connector.name());
                    }));
  }

  @Test
  void testUrlsRoundRobin() {
    var servers =
        IntStream.range(0, 3)
            .mapToObj(x -> new Server(200, "[\"connector" + x + "\"]"))
            .collect(Collectors.toList());
    try {
      var urls =
          servers.stream()
              .map(x -> "http://" + Utils.hostname() + ":" + x.port())
              .map(x -> Utils.packException(() -> new URL(x)))
              .collect(Collectors.toSet());

      var connectorClient = ConnectorClient.builder().urls(urls).build();

      var allFoundConnectorNames =
          IntStream.range(0, 20)
              .mapToObj(
                  x ->
                      Utils.packException(
                          () -> connectorClient.connectorNames().toCompletableFuture().get()))
              .flatMap(Collection::stream)
              .collect(Collectors.toSet());

      assertEquals(Set.of("connector0", "connector1", "connector2"), allFoundConnectorNames);
    } finally {
      servers.forEach(Server::close);
    }
  }

  @Test
  void testWaitConnectorInfo() throws ExecutionException, InterruptedException {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().url(workerUrl()).build();
    var exampleConnector = new HashMap<>(getExampleConnector());

    connectorClient.createConnector(connectorName, exampleConnector).toCompletableFuture().get();
    assertTrue(
        connectorClient
            .waitConnectorInfo(connectorName, x -> x.tasks().size() > 0, Duration.ofSeconds(10))
            .toCompletableFuture()
            .get());

    var errConnectorName = Utils.randomString(10);
    exampleConnector.put("connector.class", TestErrorSourceConnector.class.getName());
    connectorClient.createConnector(errConnectorName, exampleConnector).toCompletableFuture().get();

    assertFalse(
        connectorClient
            .waitConnectorInfo(errConnectorName, x -> x.tasks().size() > 0, Duration.ofSeconds(10))
            .toCompletableFuture()
            .get());
  }

  private static class Server implements AutoCloseable {
    private final HttpServer server;

    private Server(int respCode, String respBody) {
      try {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.start();
        server.createContext(
            "/",
            exchange -> {
              exchange.sendResponseHeaders(respCode, respBody == null ? 0 : respBody.length());
              try (var output = exchange.getResponseBody()) {
                if (respBody != null) output.write(respBody.getBytes(StandardCharsets.UTF_8));
              }
            });
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private int port() {
      return server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(5);
    }
  }

  private HttpRequestException getResponseException(ExecutionException executionException) {
    executionException.printStackTrace();
    assertEquals(HttpRequestException.class, executionException.getCause().getClass());
    return (HttpRequestException) executionException.getCause();
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

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
