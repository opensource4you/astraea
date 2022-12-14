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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.ConnectorConfigs;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireSingleWorkerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MetadataStorageTest extends RequireSingleWorkerCluster {

  private static final Map<String, String> KEY = Map.of("a", "b");
  private static final Map<String, String> VALUE = Map.of("c", "d");

  @Test
  void test() {
    var client = ConnectorClient.builder().url(workerUrl()).build();
    var name = Utils.randomString();
    var topic = Utils.randomString();
    client
        .createConnector(
            name,
            Map.of(
                ConnectorConfigs.CONNECTOR_CLASS_KEY,
                MySource.class.getName(),
                ConnectorConfigs.TASK_MAX_KEY,
                "1",
                ConnectorConfigs.TOPICS_KEY,
                topic))
        .toCompletableFuture()
        .join();

    Utils.sleep(Duration.ofSeconds(3));

    var status = client.connectorStatus(name).toCompletableFuture().join();
    Assertions.assertEquals("RUNNING", status.state());
    Assertions.assertEquals(1, status.tasks().size());
    status
        .tasks()
        .forEach(t -> Assertions.assertEquals("RUNNING", t.state(), t.error().orElse("no error")));

    // delete connector to check metadata storage later
    client.deleteConnector(name).toCompletableFuture().join();
    Utils.sleep(Duration.ofSeconds(3));

    client
        .createConnector(
            name,
            Map.of(
                ConnectorConfigs.CONNECTOR_CLASS_KEY,
                MySource.class.getName(),
                ConnectorConfigs.TASK_MAX_KEY,
                "1",
                ConnectorConfigs.TOPICS_KEY,
                topic))
        .toCompletableFuture()
        .join();
    Utils.sleep(Duration.ofSeconds(3));
    Assertions.assertNotEquals(0, MySource.FETCHED_METADATA.size());
    Assertions.assertEquals("d", MySource.FETCHED_METADATA.get("c"));
  }

  public static class MySource extends SourceConnector {

    private static volatile Map<String, String> FETCHED_METADATA = Map.of();

    private Configuration configuration;

    @Override
    protected void init(Configuration configuration, MetadataStorage storage) {
      this.configuration = configuration;
      FETCHED_METADATA = storage.metadata(KEY);
    }

    @Override
    protected Class<? extends SourceTask> task() {
      return MyTask.class;
    }

    @Override
    protected List<Configuration> takeConfiguration(int maxTasks) {
      return IntStream.range(0, maxTasks).mapToObj(i -> configuration).collect(Collectors.toList());
    }

    @Override
    protected List<Definition> definitions() {
      return List.of();
    }
  }

  public static class MyTask extends SourceTask {

    private boolean isDone = false;
    private Set<String> topics = Set.of();

    @Override
    protected void init(Configuration configuration) {
      topics = Set.copyOf(configuration.list(ConnectorConfigs.TOPICS_KEY, ","));
    }

    @Override
    protected Collection<Record<byte[], byte[]>> take() {
      if (isDone) return List.of();
      isDone = true;
      return topics.stream()
          .map(
              t ->
                  SourceRecord.builder()
                      .topic(t)
                      .key(new byte[100])
                      .metadataIndex(KEY)
                      .metadata(VALUE)
                      .build())
          .collect(Collectors.toList());
    }
  }
}
