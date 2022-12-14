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
package org.astraea.connector;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.ConnectorConfigs;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireWorkerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectorTest extends RequireWorkerCluster {

  @Test
  void testCustomConnectorPlugins() {
    var client = ConnectorClient.builder().url(workerUrl()).build();
    var plugins = client.plugins().toCompletableFuture().join();
    Assertions.assertNotEquals(0, plugins.size());
    Assertions.assertTrue(
        plugins.stream().anyMatch(p -> p.className().equals(MySource.class.getName())));
    Assertions.assertTrue(
        plugins.stream().anyMatch(p -> p.className().equals(MySink.class.getName())));
  }

  @Test
  void testRunCustomConnector() {
    var client = ConnectorClient.builder().url(workerUrl()).build();
    var name = Utils.randomString();
    var topic = Utils.randomString();
    var info =
        client
            .createConnector(
                name,
                Map.of(
                    ConnectorConfigs.CONNECTOR_CLASS_KEY,
                    MySource.class.getName(),
                    ConnectorConfigs.TASK_MAX_KEY,
                    "3",
                    ConnectorConfigs.TOPICS_KEY,
                    topic))
            .toCompletableFuture()
            .join();
    Assertions.assertEquals(name, info.name());
    Assertions.assertEquals(
        MySource.class.getName(), info.config().get(ConnectorConfigs.CONNECTOR_CLASS_KEY));
    Assertions.assertEquals("3", info.config().get(ConnectorConfigs.TASK_MAX_KEY));
    Assertions.assertEquals(topic, info.config().get(ConnectorConfigs.TOPICS_KEY));

    // wait for sync
    Utils.sleep(Duration.ofSeconds(3));
    Assertions.assertEquals(
        3, client.connectorInfo(name).toCompletableFuture().join().tasks().size());

    client.deleteConnector(name).toCompletableFuture().join();
    Utils.sleep(Duration.ofSeconds(3));

    Assertions.assertNotEquals(0, MySource.INIT_COUNT.get());
    Assertions.assertNotEquals(0, MySourceTask.INIT_COUNT.get());
    Assertions.assertNotEquals(0, MySource.CLOSE_COUNT.get());
    Assertions.assertNotEquals(0, MySourceTask.CLOSE_COUNT.get());

    Assertions.assertEquals(MySource.INIT_COUNT.get(), MySource.CLOSE_COUNT.get());
    Assertions.assertEquals(MySourceTask.INIT_COUNT.get(), MySourceTask.CLOSE_COUNT.get());
  }

  public static class MySource extends SourceConnector {
    private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
    private static final AtomicInteger CLOSE_COUNT = new AtomicInteger(0);

    @Override
    protected void init(Configuration configuration) {
      INIT_COUNT.incrementAndGet();
    }

    @Override
    protected void close() {
      CLOSE_COUNT.incrementAndGet();
    }

    @Override
    protected Class<? extends SourceTask> task() {
      return MySourceTask.class;
    }

    @Override
    protected List<Configuration> takeConfiguration(int maxTasks) {
      return IntStream.range(0, maxTasks)
          .mapToObj(i -> Configuration.of(Map.of()))
          .collect(Collectors.toList());
    }

    @Override
    protected List<Definition> definitions() {
      return List.of();
    }
  }

  public static class MySourceTask extends SourceTask {
    private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
    private static final AtomicInteger CLOSE_COUNT = new AtomicInteger(0);

    @Override
    protected void init(Configuration configuration) {
      INIT_COUNT.incrementAndGet();
    }

    @Override
    protected void close() {
      CLOSE_COUNT.incrementAndGet();
    }

    @Override
    protected Collection<Record<byte[], byte[]>> take() {
      return List.of();
    }
  }

  public static class MySink extends SinkConnector {

    @Override
    protected Class<? extends SinkTask> task() {
      return MySinkTask.class;
    }

    @Override
    protected List<Configuration> takeConfiguration(int maxTasks) {
      return IntStream.range(0, maxTasks)
          .mapToObj(i -> Configuration.of(Map.of()))
          .collect(Collectors.toList());
    }

    @Override
    protected List<Definition> definitions() {
      return List.of();
    }
  }

  public static class MySinkTask extends SinkTask {

    @Override
    protected void put(List<org.astraea.common.consumer.Record<byte[], byte[]>> records) {}
  }
}
