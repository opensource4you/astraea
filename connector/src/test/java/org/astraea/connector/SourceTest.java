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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.ConnectorConfigs;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireSingleWorkerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SourceTest extends RequireSingleWorkerCluster {

  private static final byte[] KEY = "key".getBytes(StandardCharsets.UTF_8);
  private static final byte[] VALUE = "value".getBytes(StandardCharsets.UTF_8);
  private static final String HEADER_KEY = "header.name";
  private static final byte[] HEADER_VALUE = "header.value".getBytes(StandardCharsets.UTF_8);

  @Test
  void testConsumeDataFromSource() {
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
                topic,
                ConnectorConfigs.KEY_CONVERTER_KEY,
                ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS,
                ConnectorConfigs.VALUE_CONVERTER_KEY,
                ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS,
                ConnectorConfigs.HEADER_CONVERTER_KEY,
                ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS))
        .toCompletableFuture()
        .join();

    Utils.sleep(Duration.ofSeconds(3));
    Assertions.assertEquals(
        "RUNNING", client.connectorStatus(name).toCompletableFuture().join().state());
    Assertions.assertEquals(
        1, client.connectorStatus(name).toCompletableFuture().join().tasks().size());
    client
        .connectorStatus(name)
        .toCompletableFuture()
        .join()
        .tasks()
        .forEach(
            t ->
                Assertions.assertEquals(
                    "RUNNING", t.state(), t.error().orElse("no error message??")));

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .build()) {
      var records = consumer.poll(1, Duration.ofSeconds(10));
      Assertions.assertEquals(1, records.size());
      Assertions.assertArrayEquals(KEY, records.get(0).key());
      Assertions.assertArrayEquals(VALUE, records.get(0).value());
      Assertions.assertEquals(1, records.get(0).headers().size());
      Assertions.assertEquals(HEADER_KEY, records.get(0).headers().get(0).key());
      Assertions.assertArrayEquals(
          HEADER_VALUE,
          records.get(0).headers().get(0).value(),
          new String(records.get(0).headers().get(0).value(), StandardCharsets.UTF_8));
    }
  }

  public static class MySource extends SourceConnector {

    private Configuration configuration = Configuration.EMPTY;

    @Override
    protected void init(Configuration configuration, MetadataStorage storage) {
      this.configuration = configuration;
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

    private Set<String> topics = Set.of();
    private boolean isDone = false;

    @Override
    protected void init(Configuration configuration) {
      topics = Set.copyOf(configuration.list(ConnectorConfigs.TOPICS_KEY, ","));
    }

    @Override
    protected Collection<Record<byte[], byte[]>> take() throws InterruptedException {
      if (isDone) return List.of();
      isDone = true;
      return topics.stream()
          .map(
              t ->
                  SourceRecord.builder()
                      .key(KEY)
                      .value(VALUE)
                      .topic(t)
                      .headers(List.of(Header.of(HEADER_KEY, HEADER_VALUE)))
                      .build())
          .collect(Collectors.toList());
    }
  }
}
