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
package org.astraea.common.partitioner.smooth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.common.FutureUtils;
import org.astraea.common.Utils;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.consumer.Header;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Serializer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SmoothWeightRoundRobinDispatchTest extends RequireBrokerCluster {
  private final String brokerList = bootstrapServers();
  private final AsyncAdmin admin = AsyncAdmin.of(bootstrapServers());

  private Properties initProConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(
        ProducerConfig.PARTITIONER_CLASS_CONFIG, SmoothWeightRoundRobinDispatcher.class.getName());
    props.put("producerID", 1);
    var file =
        new File(
            SmoothWeightRoundRobinDispatchTest.class.getResource("").getPath()
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
        SmoothWeightRoundRobinDispatchTest.class.getResource("").getPath()
            + "PartitionerConfigTest");
    return props;
  }

  @Test
  void testPartitioner() throws ExecutionException, InterruptedException {
    var topicName = "address";
    admin.creator().topic(topicName).numberOfPartitions(10).run().toCompletableFuture().get();
    var key = "tainan";
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
      var i = 0;
      while (i < 300) {
        var metadata =
            producer
                .sender()
                .topic(topicName)
                .key(key)
                .timestamp(timestamp)
                .headers(List.of(header))
                .run()
                .toCompletableFuture()
                .get();
        assertEquals(topicName, metadata.topic());
        assertEquals(timestamp, metadata.timestamp());
        i++;
      }
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
    Utils.sleep(Duration.ofSeconds(1));
    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .keyDeserializer(Deserializer.STRING)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(20));
      var recordsCount = records.size();
      while (recordsCount < 300) {
        recordsCount += consumer.poll(Duration.ofSeconds(20)).size();
      }
      assertEquals(300, recordsCount);
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
  void testMultipleProducer() throws ExecutionException, InterruptedException {
    var topicName = "addr";
    admin.creator().topic(topicName).numberOfPartitions(10).run().toCompletableFuture().get();
    var key = "tainan";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());

    FutureUtils.sequence(
            IntStream.range(0, 10)
                .mapToObj(
                    ignored ->
                        CompletableFuture.runAsync(
                            producerThread(
                                Producer.builder()
                                    .keySerializer(Serializer.STRING)
                                    .configs(
                                        initProConfig().entrySet().stream()
                                            .collect(
                                                Collectors.toMap(
                                                    e -> e.getKey().toString(),
                                                    e -> e.getValue().toString())))
                                    .build(),
                                topicName,
                                key,
                                header,
                                timestamp)))
                .collect(Collectors.toUnmodifiableList()))
        .get();

    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .keyDeserializer(Deserializer.STRING)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(20));
      var recordsCount = records.size();
      while (recordsCount < 1000) {
        recordsCount += consumer.poll(Duration.ofSeconds(20)).size();
      }
      assertEquals(1000, recordsCount);
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
  void testJmxConfig() throws ExecutionException, InterruptedException {
    var props = initProConfig();
    var file =
        new File(
            SmoothWeightRoundRobinDispatchTest.class.getResource("").getPath()
                + "PartitionerConfigTest");
    try {
      var fileWriter = new FileWriter(file);
      fileWriter.write(
          "broker.0.jmx.port="
              + jmxServiceURL().getPort()
              + "\n"
              + "broker.1.jmx.port="
              + jmxServiceURL().getPort()
              + "\n"
              + "broker.2.jmx.port="
              + jmxServiceURL().getPort()
              + "\n");
      fileWriter.flush();
      fileWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    var topicName = "addressN";
    admin.creator().topic(topicName).numberOfPartitions(10).run().toCompletableFuture().get();
    var key = "tainan";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());

    try (var producer =
        Producer.builder()
            .keySerializer(Serializer.STRING)
            .configs(
                props.entrySet().stream()
                    .collect(
                        Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString())))
            .build()) {
      var metadata =
          producer
              .sender()
              .topic(topicName)
              .key(key)
              .timestamp(timestamp)
              .headers(List.of(header))
              .run()
              .toCompletableFuture()
              .get();
      assertEquals(topicName, metadata.topic());
      assertEquals(timestamp, metadata.timestamp());
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private Runnable producerThread(
      Producer<String, byte[]> producer, String topic, String key, Header header, long timeStamp) {
    return () -> {
      try (producer) {
        var i = 0;
        while (i <= 99) {
          producer
              .sender()
              .topic(topic)
              .key(key)
              .timestamp(timeStamp)
              .headers(List.of(header))
              .run();
          i++;
        }
        producer.flush();
      }
    };
  }

  @Test
  public void testGetAndChoose() {
    var topic = "test";
    var smoothWeight = new SmoothWeightRoundRobin(Map.of(1, 5.0, 2, 3.0, 3, 1.0));
    var node1 = Mockito.mock(NodeInfo.class);
    Mockito.when(node1.id()).thenReturn(1);
    var re1 = ReplicaInfo.of(topic, 0, node1, true, true, false);

    var node2 = Mockito.mock(NodeInfo.class);
    Mockito.when(node2.id()).thenReturn(2);
    var re2 = ReplicaInfo.of(topic, 1, node2, true, true, false);

    var node3 = Mockito.mock(NodeInfo.class);
    Mockito.when(node3.id()).thenReturn(3);
    var re3 = ReplicaInfo.of(topic, 2, node3, true, true, false);
    var testCluster = ClusterInfo.of(List.of(re1, re2, re3));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(2, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(3, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(2, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(3, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
  }
}
