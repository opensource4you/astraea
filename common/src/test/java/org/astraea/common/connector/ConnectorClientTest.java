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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.connector.impl.TestErrorSourceConnector;
import org.astraea.common.connector.impl.TestTextSourceConnector;
import org.astraea.common.http.HttpRequestException;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConnectorClientTest {

  private static final Service SERVICE =
      Service.builder().numberOfWorkers(1).numberOfBrokers(1).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testValidate() {
    var connectorClient = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    var validation =
        connectorClient
            .validate(TestTextSourceConnector.class.getName(), Map.of())
            .toCompletableFuture()
            .join();
    assertNotEquals(0, validation.errorCount());
  }

  @Test
  void testWorkerStatus() {
    var connectorClient = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    // make all workers busy
    connectorClient
        .createConnector(Utils.randomString(), getExampleConnector())
        .toCompletableFuture()
        .join();
    Utils.sleep(Duration.ofSeconds(3));

    var workers = connectorClient.activeWorkers().toCompletableFuture().join();
    Assertions.assertNotEquals(0, workers.size());
    Assertions.assertEquals(
        workers.stream()
            .map(
                w ->
                    Utils.packException(
                        () -> new URL("http://" + w.hostname() + ":" + w.port() + "/")))
            .collect(Collectors.toSet()),
        Set.copyOf(SERVICE.workerUrls()));
    workers.forEach(w -> Assertions.assertNotEquals(0, w.numberOfConnectors() + w.numberOfTasks()));
  }

  @Test
  void testConnectors() {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    var connectors = connectorClient.connectorNames().toCompletableFuture().join();
    assertFalse(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    connectorClient
        .createConnector(connectorName, getExampleConnector())
        .toCompletableFuture()
        .join();
    connectors = connectorClient.connectorNames().toCompletableFuture().join();
    assertTrue(connectors.stream().anyMatch(x -> x.equals(connectorName)));
  }

  @Test
  void testCreateConnector() {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().urls(List.copyOf(SERVICE.workerUrls())).build();
    var exampleConnector = new HashMap<>(getExampleConnector());
    exampleConnector.put("tasks.max", "3");

    var createdConnectorInfo =
        connectorClient
            .createConnector(connectorName, exampleConnector)
            .toCompletableFuture()
            .join();
    assertEquals(connectorName, createdConnectorInfo.name());
    var config = createdConnectorInfo.config();
    assertEquals("3", config.get("tasks.max"));
    assertEquals("myTopic", config.get("topics"));
    assertEquals(TestTextSourceConnector.class.getName(), config.get("connector.class"));

    assertTrue(
        connectorClient
            .waitConnector(connectorName, x -> x.tasks().size() > 0, Duration.ofSeconds(30))
            .toCompletableFuture()
            .join(),
        "connector: "
            + connectorClient.connectorStatus(connectorName).toCompletableFuture().join());
    var connectorInfo = connectorClient.connectorStatus(connectorName).toCompletableFuture().join();
    assertEquals(3, connectorInfo.tasks().size());
    assertTrue(
        connectorInfo.tasks().stream().allMatch(x -> connectorName.equals(x.connectorName())));
    assertEquals(3, connectorInfo.tasks().stream().map(TaskStatus::id).distinct().count());
    connectorInfo.tasks().forEach(t -> assertNotEquals(0, t.configs().size()));
  }

  @Test
  void testUpdateConnector() {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    var exampleConnector = getExampleConnector();

    var connector =
        connectorClient
            .createConnector(connectorName, exampleConnector)
            .toCompletableFuture()
            .join();
    assertEquals("1", connector.config().get("tasks.max"));
    assertEquals("myTopic", connector.config().get("topics"));
    var connectors = connectorClient.connectorNames().toCompletableFuture().join();
    assertTrue(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    var updateConfig = new HashMap<>(exampleConnector);
    updateConfig.put("tasks.max", "2");
    updateConfig.put("topics", "myTopic2");

    connector =
        connectorClient.updateConnector(connectorName, updateConfig).toCompletableFuture().join();
    assertEquals("2", connector.config().get("tasks.max"));
    assertEquals("myTopic2", connector.config().get("topics"));

    // wait for syncing configs and re-balance
    Utils.sleep(Duration.ofSeconds(7));
    var status = connectorClient.connectorStatus(connectorName).toCompletableFuture().join();
    assertEquals(connectorName, status.name());
    assertEquals("RUNNING", status.state());
    assertEquals("source", status.type().get());
    assertEquals(2, status.tasks().size());
    assertNotEquals(0, status.configs().size());
    status.tasks().forEach(t -> assertEquals("RUNNING", t.state()));
    status.tasks().forEach(t -> assertNotEquals(0, t.configs().size()));
  }

  @Test
  void testDeleteConnector() {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    var exampleConnector = getExampleConnector();

    connectorClient.createConnector(connectorName, exampleConnector).toCompletableFuture().join();
    var connectors = connectorClient.connectorNames().toCompletableFuture().join();
    assertTrue(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    connectorClient.deleteConnector(connectorName).toCompletableFuture().join();
    connectors = connectorClient.connectorNames().toCompletableFuture().join();
    assertFalse(connectors.stream().anyMatch(x -> x.equals(connectorName)));

    var CompletionException =
        assertThrows(
            CompletionException.class,
            () -> connectorClient.deleteConnector("unknown").toCompletableFuture().join());

    var exception = getResponseException(CompletionException);

    // In distribution mode, Request forward to another node will throw 500, otherwise 404.
    assertTrue(exception.getMessage().contains("unknown not found"));
  }

  @Test
  void testPlugin() {
    var connectorClient = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    var plugins = connectorClient.plugins().toCompletableFuture().join();
    assertNotEquals(0, plugins.size());
    assertTrue(
        plugins.stream()
            .anyMatch(x -> TestTextSourceConnector.class.getName().equals(x.className())));
    plugins.forEach(p -> assertNotEquals(0, p.definitions().size()));
  }

  @Test
  void testUrls() {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().urls(List.copyOf(SERVICE.workerUrls())).build();
    connectorClient
        .createConnector(connectorName, getExampleConnector())
        .toCompletableFuture()
        .join();

    // wait for syncing configs
    Utils.sleep(Duration.ofSeconds(2));

    IntStream.range(0, 15)
        .forEach(
            x ->
                Utils.packException(
                    () -> {
                      var connector =
                          connectorClient
                              .connectorStatus(connectorName)
                              .toCompletableFuture()
                              .join();
                      assertEquals(connectorName, connector.name());
                    }));
  }

  @Test
  void testUrlsRoundRobin() {
    var servers =
        IntStream.range(0, 3).mapToObj(x -> new Server(200, "[\"connector" + x + "\"]")).toList();
    try {
      var urls =
          servers.stream()
              .map(x -> "http://" + Utils.hostname() + ":" + x.port())
              .map(x -> Utils.packException(() -> new URL(x)))
              .toList();

      var connectorClient = ConnectorClient.builder().urls(urls).build();

      var allFoundConnectorNames =
          IntStream.range(0, 20)
              .mapToObj(
                  x ->
                      Utils.packException(
                          () -> connectorClient.connectorNames().toCompletableFuture().join()))
              .flatMap(Collection::stream)
              .collect(Collectors.toSet());

      assertEquals(Set.of("connector0", "connector1", "connector2"), allFoundConnectorNames);
    } finally {
      servers.forEach(Server::close);
    }
  }

  @Test
  void testWaitConnectorInfo() {
    var connectorName = Utils.randomString(10);
    var connectorClient = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    var exampleConnector = new HashMap<>(getExampleConnector());

    connectorClient.createConnector(connectorName, exampleConnector).toCompletableFuture().join();
    assertTrue(
        connectorClient
            .waitConnector(connectorName, x -> x.tasks().size() > 0, Duration.ofSeconds(10))
            .toCompletableFuture()
            .join());

    var errConnectorName = Utils.randomString(10);
    exampleConnector.put("connector.class", TestErrorSourceConnector.class.getName());
    connectorClient
        .createConnector(errConnectorName, exampleConnector)
        .toCompletableFuture()
        .join();

    assertFalse(
        connectorClient
            .waitConnector(errConnectorName, x -> x.tasks().size() > 0, Duration.ofSeconds(10))
            .toCompletableFuture()
            .join());
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

  private HttpRequestException getResponseException(Exception CompletionException) {
    CompletionException.printStackTrace();
    assertEquals(HttpRequestException.class, CompletionException.getCause().getClass());
    return (HttpRequestException) CompletionException.getCause();
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
