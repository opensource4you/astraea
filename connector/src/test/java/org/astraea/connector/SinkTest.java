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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.connector.ConnectorConfigs;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SinkTest {

  private static final Service SERVICE =
      Service.builder().numberOfWorkers(1).numberOfBrokers(1).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void test() {
    var topic = Utils.randomString(10);
    var name = Utils.randomString(10);
    try (var producer = Producer.of(SERVICE.bootstrapServers())) {
      IntStream.range(0, 10)
          .forEach(
              i ->
                  producer.send(
                      List.of(
                          Record.builder()
                              .topic(topic)
                              .key(String.valueOf(i).getBytes(StandardCharsets.UTF_8))
                              .build())));
      producer.flush();
    }
    var client = ConnectorClient.builder().url(SERVICE.workerUrl()).build();
    client
        .createConnector(
            name,
            Map.of(
                ConnectorConfigs.CONNECTOR_CLASS_KEY,
                MySink.class.getName(),
                ConnectorConfigs.TASK_MAX_KEY,
                "1",
                ConnectorConfigs.TOPICS_KEY,
                topic,
                ConnectorConfigs.KEY_CONVERTER_KEY,
                ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS,
                ConnectorConfigs.VALUE_CONVERTER_KEY,
                ConnectorConfigs.BYTE_ARRAY_CONVERTER_CLASS))
        .toCompletableFuture()
        .join();
    Utils.sleep(Duration.ofSeconds(5));
    Assertions.assertEquals(1, MySinkTask.LOOP.get());
    Assertions.assertEquals(15, MySinkTask.COUNT.get());
  }

  public static class MySink extends SinkConnector {

    private Configuration configuration;

    @Override
    protected void init(Configuration configuration, SinkContext context) {
      this.configuration = configuration;
    }

    @Override
    protected Class<? extends SinkTask> task() {
      return MySinkTask.class;
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

  public static class MySinkTask extends SinkTask {
    private static final AtomicInteger LOOP = new AtomicInteger();
    private static final AtomicInteger COUNT = new AtomicInteger();
    private SinkTaskContext context;

    private final Set<TopicPartition> tps = new HashSet<>();

    @Override
    protected void init(Configuration configuration, SinkTaskContext context) {
      this.context = context;
    }

    @Override
    protected void put(List<org.astraea.common.consumer.Record<byte[], byte[]>> records) {
      tps.addAll(records.stream().map(org.astraea.common.consumer.Record::topicPartition).toList());
      COUNT.addAndGet(records.size());
      if (COUNT.get() == 10 && LOOP.get() == 0) {
        context.offset(tps.stream().collect(Collectors.toUnmodifiableMap(tp -> tp, ignored -> 5L)));
        tps.clear();
        LOOP.incrementAndGet();
      }
    }
  }
}
