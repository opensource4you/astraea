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
package org.astraea.common.partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.common.Configuration;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoBuilder;
import org.astraea.common.producer.Metadata;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class PartitionerTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(1).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  private final String SMOOTH_ROUND_ROBIN =
      "org.astraea.common.partitioner.SmoothWeightRoundRobinPartitioner";
  private final String STRICT_ROUND_ROBIN = "org.astraea.common.partitioner.StrictCostPartitioner";

  @Test
  void testUpdateClusterInfo() {

    try (var partitioner =
        new Partitioner() {
          @Override
          public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
            Assertions.assertNotNull(clusterInfo);
            return 0;
          }
        }) {

      partitioner.configure(
          Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVICE.bootstrapServers()));
      Assertions.assertNotNull(partitioner.admin);
      Utils.sleep(Duration.ofSeconds(3));
      Assertions.assertNotEquals(0, partitioner.clusterInfo.nodes().size());
    }
  }

  @Test
  void testNullKey() {
    var count = new AtomicInteger();
    Partitioner partitioner =
        new Partitioner() {
          @Override
          public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
            Assertions.assertNull(key);
            Assertions.assertNull(value);
            count.incrementAndGet();
            return 0;
          }

          @Override
          public void configure(Configuration config) {
            count.incrementAndGet();
          }
        };
    Assertions.assertEquals(0, count.get());
    partitioner.configure(Map.of("a", "b"));

    Assertions.assertEquals(1, count.get());
    partitioner.partition("t", null, null, ClusterInfoBuilder.builder().build());
    Assertions.assertEquals(2, count.get());
  }

  @ParameterizedTest
  @ValueSource(strings = {SMOOTH_ROUND_ROBIN, STRICT_ROUND_ROBIN})
  void multipleThreadTest(String className) throws IOException {
    var topicName = "address";
    createTopic(topicName);
    var key = "tainan";
    var value = "shanghai";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());
    try (var producer =
        Producer.builder()
            .keySerializer(Serializer.STRING)
            .configs(
                put(initProConfig(), className).entrySet().stream()
                    .collect(
                        Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString())))
            .build()) {
      Runnable runnable =
          () -> {
            Partitioner.beginInterdependent(instanceOfProducer(producer));
            var exceptPartition =
                producerSend(producer, topicName, key, value, timestamp, header).partition();
            IntStream.range(0, 10)
                .forEach(
                    i -> {
                      Metadata metadata;
                      metadata = producerSend(producer, topicName, key, value, timestamp, header);
                      assertEquals(topicName, metadata.topic());
                      assertEquals(timestamp, metadata.timestamp());
                      assertEquals(exceptPartition, metadata.partition());
                    });
            Partitioner.endInterdependent(instanceOfProducer(producer));
          };
      Partitioner.beginInterdependent(instanceOfProducer(producer));

      var fs =
          IntStream.range(0, 10)
              .mapToObj(i -> CompletableFuture.runAsync(runnable))
              .collect(Collectors.toList());

      var exceptPartition =
          producerSend(producer, topicName, key, value, timestamp, header).partition();
      IntStream.range(0, 10)
          .forEach(
              i -> {
                Metadata metadata;
                metadata = producerSend(producer, topicName, key, value, timestamp, header);
                assertEquals(topicName, metadata.topic());
                assertEquals(timestamp, metadata.timestamp());
                assertEquals(exceptPartition, metadata.partition());
              });
      Partitioner.endInterdependent(instanceOfProducer(producer));
      fs.forEach(CompletableFuture::join);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {SMOOTH_ROUND_ROBIN, STRICT_ROUND_ROBIN})
  void interdependentTest(String className) throws IOException {
    var topicName = "address";
    createTopic(topicName);
    var key = "tainan";
    var value = "shanghai";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());
    try (var producer =
        Producer.builder()
            .keySerializer(Serializer.STRING)
            .configs(
                put(initProConfig(), className).entrySet().stream()
                    .collect(
                        Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString())))
            .build()) {
      IntStream.range(0, 240)
          .forEach(
              i -> {
                Metadata metadata;
                metadata = producerSend(producer, topicName, key, value, timestamp, header);
                assertEquals(topicName, metadata.topic());
                assertEquals(timestamp, metadata.timestamp());
              });
      Partitioner.beginInterdependent(instanceOfProducer(producer));
      var exceptPartition =
          producerSend(producer, topicName, key, value, timestamp, header).partition();
      IntStream.range(0, 99)
          .forEach(
              i -> {
                Metadata metadata;
                metadata = producerSend(producer, topicName, key, value, timestamp, header);
                assertEquals(topicName, metadata.topic());
                assertEquals(timestamp, metadata.timestamp());
                assertEquals(exceptPartition, metadata.partition());
              });
      Partitioner.endInterdependent(instanceOfProducer(producer));
      IntStream.range(0, 2400)
          .forEach(
              i -> {
                Metadata metadata;
                metadata = producerSend(producer, topicName, key, value, timestamp, header);
                assertEquals(topicName, metadata.topic());
                assertEquals(timestamp, metadata.timestamp());
              });
      Partitioner.beginInterdependent(instanceOfProducer(producer));
      var exceptPartitionSec =
          producerSend(producer, topicName, key, value, timestamp, header).partition();
      IntStream.range(0, 99)
          .forEach(
              i -> {
                Metadata metadata;
                metadata = producerSend(producer, topicName, key, value, timestamp, header);
                assertEquals(topicName, metadata.topic());
                assertEquals(timestamp, metadata.timestamp());
                assertEquals(exceptPartitionSec, metadata.partition());
              });
      Partitioner.endInterdependent(instanceOfProducer(producer));
    }
  }

  private void createTopic(String topic) {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(9).run().toCompletableFuture().join();
    }
  }

  private Metadata producerSend(
      Producer<String, byte[]> producer,
      String topicName,
      String key,
      String value,
      long timestamp,
      Header header) {
    return producer
        .send(
            Record.builder()
                .topic(topicName)
                .key(key)
                .value(value.getBytes())
                .timestamp(timestamp)
                .headers(List.of(header))
                .build())
        .toCompletableFuture()
        .join();
  }

  @SuppressWarnings("unchecked")
  private org.apache.kafka.clients.producer.Producer<Key, Value> instanceOfProducer(Object object) {
    var producer = Utils.member(object, "kafkaProducer");
    return producer instanceof org.apache.kafka.clients.producer.Producer
        ? (org.apache.kafka.clients.producer.Producer<Key, Value>) producer
        : null;
  }

  private Properties put(Properties properties, String name) {
    properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, name);
    return properties;
  }

  private Properties initProConfig() throws IOException {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVICE.bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(
        ProducerConfig.PARTITIONER_CLASS_CONFIG, SmoothWeightRoundRobinPartitioner.class.getName());
    props.put("producerID", 1);
    var file =
        Path.of(
            Objects.requireNonNull(PartitionerTest.class.getResource("")).getPath()
                + "PartitionerConfigTest");
    try (var fileWriter = Files.newBufferedWriter(file)) {
      fileWriter.write("jmx.port=" + SERVICE.jmxServiceURL().getPort() + "\n");
      fileWriter.write("broker.0.jmx.port=" + SERVICE.jmxServiceURL().getPort() + "\n");
      fileWriter.write("broker.1.jmx.port=" + SERVICE.jmxServiceURL().getPort() + "\n");
      fileWriter.write("broker.2.jmx.port=" + SERVICE.jmxServiceURL().getPort());
      fileWriter.flush();
    }
    props.put(
        "partitioner.config",
        Objects.requireNonNull(PartitionerTest.class.getResource("")).getPath()
            + "PartitionerConfigTest");
    return props;
  }
}
