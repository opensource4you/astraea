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
package org.astraea.app.partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.Key;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.common.Utils;
import org.astraea.app.consumer.Consumer;
import org.astraea.app.consumer.Deserializer;
import org.astraea.app.consumer.Header;
import org.astraea.app.partitioner.smooth.SmoothWeightRoundRobinDispatcher;
import org.astraea.app.producer.Metadata;
import org.astraea.app.producer.Producer;
import org.astraea.app.producer.Serializer;
import org.astraea.app.service.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DispatcherTest extends RequireSingleBrokerCluster {
  @Test
  void testNullKey() {
    var count = new AtomicInteger();
    var dispatcher =
        new Dispatcher() {
          @Override
          public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
            Assertions.assertEquals(0, Objects.requireNonNull(key).length);
            Assertions.assertEquals(0, Objects.requireNonNull(value).length);
            count.incrementAndGet();
            return 0;
          }

          @Override
          public void configure(Configuration config) {
            Dispatcher.INTERDEPENDENT.putIfAbsent(this.hashCode(), new ThreadLocal<>());
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
          public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
            return 0;
          }

          @Override
          public void configure(Configuration config) {
            Dispatcher.INTERDEPENDENT.putIfAbsent(this.hashCode(), new ThreadLocal<>());
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

  @Test
  void testCloseDispatcher() {
    var dispatcher1 =
        new Dispatcher() {
          @Override
          public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
            return 0;
          }
        };
    var dispatcher2 =
        new Dispatcher() {
          @Override
          public int partition(String topic, byte[] key, byte[] value, ClusterInfo clusterInfo) {
            return 0;
          }
        };
    dispatcher1.configure(Map.of());
    dispatcher2.configure(Map.of());
    dispatcher1.close();
    Assertions.assertNotNull(Dispatcher.INTERDEPENDENT.get(dispatcher2.hashCode()));
    Assertions.assertNull(Dispatcher.INTERDEPENDENT.get(dispatcher1.hashCode()));
    dispatcher2.close();
    Assertions.assertNull(Dispatcher.INTERDEPENDENT.get(dispatcher1.hashCode()));
  }

  @Test
  void endErrorTest(){
      var admin = Admin.of(bootstrapServers());
      var topicName = "address";
      admin.creator().topic(topicName).numberOfPartitions(9).create();
      try (var producer =
                   Producer.builder()
                           .keySerializer(Serializer.STRING)
                           .configs(
                                   initProConfig().entrySet().stream()
                                           .collect(
                                                   Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString())))
                           .build()) {
          Assertions.assertThrows(RuntimeException.class,()->Dispatcher.endInterdependent(instanceOfProducer(producer)));
      }
  }

  @Test
  void multipleThreadTest(){
      var admin = Admin.of(bootstrapServers());
      var topicName = "address";
      admin.creator().topic(topicName).numberOfPartitions(9).create();
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
          Runnable runnable = ()->{
              Dispatcher.beginInterdependent(instanceOfProducer(producer));
              Dispatcher.endInterdependent(instanceOfProducer(producer));
          };
          Dispatcher.beginInterdependent(instanceOfProducer(producer));
          new Thread(runnable).start();
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
      }
  }

  @Test
  void interdependentTest() {
    var admin = Admin.of(bootstrapServers());
    var topicName = "address";
    admin.creator().topic(topicName).numberOfPartitions(9).create();
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

    Utils.sleep(Duration.ofSeconds(1));
    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .fromBeginning()
            .keyDeserializer(Deserializer.STRING)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(20));
      var recordsCount = records.size();
      while (recordsCount < 2840) {
        recordsCount += consumer.poll(Duration.ofSeconds(20)).size();
      }
      assertEquals(2840, recordsCount);
      var record = records.iterator().next();
      assertEquals(topicName, record.topic());
      assertEquals("tainan", record.key());
      assertEquals(1, record.headers().size());
      var actualHeader = record.headers().iterator().next();
      assertEquals(header.key(), actualHeader.key());
      Assertions.assertArrayEquals(header.value(), actualHeader.value());
    }
  }

  @Test
  void multipleProducers() {
    var admin = Admin.of(bootstrapServers());
    var topicName = "city";
    admin.creator().topic(topicName).numberOfPartitions(9).create();
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
      var producer2 =
          Producer.builder()
              .keySerializer(Serializer.STRING)
              .configs(
                  initProConfig().entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              e -> e.getKey().toString(), e -> e.getValue().toString())))
              .build();
      Dispatcher.beginInterdependent(instanceOfProducer(producer));
      Dispatcher.beginInterdependent(instanceOfProducer(producer2));
      var exceptPartition =
          producerSend(producer, topicName, key, value, timestamp, header).partition();
      IntStream.range(0, 99)
          .forEach(
              i -> {
                Metadata metadata;
                metadata = producerSend(producer, topicName, key, value, timestamp, header);
                var metadata2 = producerSend(producer2, topicName, key, value, timestamp, header);
                assertEquals(topicName, metadata.topic());
                assertEquals(timestamp, metadata.timestamp());
                assertEquals(exceptPartition, metadata.partition());
                assertNotEquals(metadata2.partition(), metadata.partition());
              });
      Dispatcher.endInterdependent(instanceOfProducer(producer2));
      Dispatcher.endInterdependent(instanceOfProducer(producer));
      producer2.close();
    }
  }

  private Metadata producerSend(
      Producer<String, byte[]> producer,
      String topicName,
      String key,
      String value,
      long timestamp,
      Header header) {
    try {
      return producer
          .sender()
          .topic(topicName)
          .key(key)
          .value(value.getBytes())
          .timestamp(timestamp)
          .headers(List.of(header))
          .run()
          .toCompletableFuture()
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private org.apache.kafka.clients.producer.Producer<Key, Value> instanceOfProducer(Object object) {
    var producer = Utils.reflectionAttribute(object, "kafkaProducer");
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
    props.put(
        ProducerConfig.PARTITIONER_CLASS_CONFIG, SmoothWeightRoundRobinDispatcher.class.getName());
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
