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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.Key;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.cost.Configuration;
import org.astraea.common.producer.Metadata;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;
import org.astraea.it.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

public class DispatcherTest extends RequireSingleBrokerCluster {
  @Test
  void testNullKey() {
    var count = new AtomicInteger();
    var dispatcher =
        new Dispatcher() {
          @Override
          public int partition(
              String topic, byte[] key, byte[] value, ClusterInfo<ReplicaInfo> clusterInfo) {
            Assertions.assertEquals(0, Objects.requireNonNull(key).length);
            Assertions.assertEquals(0, Objects.requireNonNull(value).length);
            count.incrementAndGet();
            return 0;
          }

          @Override
          public void configure(Configuration config) {
            count.incrementAndGet();
          }
        };
    Assertions.assertEquals(0, count.get());
    dispatcher.configure(Map.of("a", "b"));

    Assertions.assertEquals(1, count.get());
    dispatcher.partition(
        "t", null, null, null, null, new Cluster("aa", List.of(), List.of(), Set.of(), Set.of()));
    Assertions.assertEquals(2, count.get());
  }

  @Test
  void testClusterCache() {
    var dispatcher =
        new Dispatcher() {
          @Override
          public int partition(
              String topic, byte[] key, byte[] value, ClusterInfo<ReplicaInfo> clusterInfo) {
            return 0;
          }
        };
    dispatcher.configure(Map.of());
    var initialCount = Dispatcher.CLUSTER_CACHE.size();
    var cluster = new Cluster("aa", List.of(), List.of(), Set.of(), Set.of());
    dispatcher.partition("topic", "a", new byte[0], "v", new byte[0], cluster);
    Assertions.assertEquals(initialCount + 1, Dispatcher.CLUSTER_CACHE.size());
    dispatcher.partition("topic", "a", new byte[0], "v", new byte[0], cluster);
    Assertions.assertEquals(initialCount + 1, Dispatcher.CLUSTER_CACHE.size());
  }

  @RepeatedTest(5)
  void multipleThreadTest() {
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
                initProConfig().entrySet().stream()
                    .collect(
                        Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString())))
            .build()) {
      Runnable runnable =
          () -> {
            Dispatcher.beginInterdependent(instanceOfProducer(producer));
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
            Dispatcher.endInterdependent(instanceOfProducer(producer));
          };
      Dispatcher.beginInterdependent(instanceOfProducer(producer));

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
      Dispatcher.endInterdependent(instanceOfProducer(producer));
      fs.forEach(CompletableFuture::join);
    }
  }

  @Test
  void interdependentTest() {
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
                initProConfig().entrySet().stream()
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
      Dispatcher.beginInterdependent(instanceOfProducer(producer));
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
      Dispatcher.endInterdependent(instanceOfProducer(producer));
      IntStream.range(0, 2400)
          .forEach(
              i -> {
                Metadata metadata;
                metadata = producerSend(producer, topicName, key, value, timestamp, header);
                assertEquals(topicName, metadata.topic());
                assertEquals(timestamp, metadata.timestamp());
              });
      Dispatcher.beginInterdependent(instanceOfProducer(producer));
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
      Dispatcher.endInterdependent(instanceOfProducer(producer));
    }
  }

  private void createTopic(String topic) {
    try (var admin = Admin.of(bootstrapServers())) {
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

  private Properties initProConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    // TODO: add smooth dispatch to this test
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, StrictCostDispatcher.class.getName());
    props.put("producerID", 1);
    var file =
        new File(
            Objects.requireNonNull(DispatcherTest.class.getResource("")).getPath()
                + "PartitionerConfigTest");
    try {
      var fileWriter = new FileWriter(file);
      fileWriter.write("jmx.port=" + jmxServiceURL().getPort() + "\n");
      fileWriter.write("broker.0.jmx.port=" + jmxServiceURL().getPort() + "\n");
      fileWriter.write("broker.1.jmx.port=" + jmxServiceURL().getPort() + "\n");
      fileWriter.write("broker.2.jmx.port=" + jmxServiceURL().getPort());
      fileWriter.flush();
      fileWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    props.put(
        "partitioner.config",
        Objects.requireNonNull(DispatcherTest.class.getResource("")).getPath()
            + "PartitionerConfigTest");
    return props;
  }
}
