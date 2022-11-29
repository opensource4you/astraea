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
package org.astraea.common.producer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.Deserializer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class ProducerTest extends RequireBrokerCluster {

  @Test
  void testSender() {
    var topicName = "testSender-" + System.currentTimeMillis();
    var key = "key";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());
    try (var producer =
        Producer.builder()
            .bootstrapServers(bootstrapServers())
            .keySerializer(Serializer.STRING)
            .build()) {
      Assertions.assertFalse(producer.transactional());
      var metadata =
          producer
              .send(
                  Record.builder()
                      .topic(topicName)
                      .key(key)
                      .timestamp(timestamp)
                      .headers(List.of(header))
                      .build())
              .toCompletableFuture()
              .join();
      Assertions.assertEquals(topicName, metadata.topic());
      Assertions.assertEquals(timestamp, metadata.timestamp());
    }

    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .keyDeserializer(Deserializer.STRING)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(10));
      Assertions.assertEquals(1, records.size());
      var record = records.iterator().next();
      Assertions.assertEquals(topicName, record.topic());
      Assertions.assertEquals("key", record.key());
      Assertions.assertEquals(1, record.headers().size());
      var actualHeader = record.headers().iterator().next();
      Assertions.assertEquals(header.key(), actualHeader.key());
      Assertions.assertArrayEquals(header.value(), actualHeader.value());
    }
  }

  @Test
  void testTransaction() {
    var topicName = "testTransaction-" + System.currentTimeMillis();
    var key = "key";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());
    try (var producer =
        Producer.builder()
            .bootstrapServers(bootstrapServers())
            .keySerializer(Serializer.STRING)
            .buildTransactional()) {
      Assertions.assertTrue(producer.transactional());
      var senders = new ArrayList<Record<String, byte[]>>(3);
      while (senders.size() < 3) {
        senders.add(
            Record.builder()
                .topic(topicName)
                .key(key)
                .timestamp(timestamp)
                .headers(List.of(header))
                .build());
      }
      producer.send(senders);
    }

    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .keyDeserializer(Deserializer.STRING)
            .config(
                ConsumerConfigs.ISOLATION_LEVEL_CONFIG, ConsumerConfigs.ISOLATION_LEVEL_COMMITTED)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(10));
      Assertions.assertEquals(3, records.size());
    }
  }

  @ParameterizedTest
  @MethodSource("offerProducers")
  void testSingleSend(Producer<byte[], byte[]> producer) {
    var topic = Utils.randomString(10);

    producer
        .send(Record.builder().topic(topic).value(new byte[10]).build())
        .toCompletableFuture()
        .join();

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .config(
                ConsumerConfigs.ISOLATION_LEVEL_CONFIG,
                producer.transactional()
                    ? ConsumerConfigs.ISOLATION_LEVEL_COMMITTED
                    : ConsumerConfigs.ISOLATION_LEVEL_UNCOMMITTED)
            .build()) {
      Assertions.assertEquals(1, consumer.poll(Duration.ofSeconds(10)).size());
    }
  }

  @ParameterizedTest
  @MethodSource("offerProducers")
  void testMultiplesSend(Producer<byte[], byte[]> producer) throws InterruptedException {
    var topic = Utils.randomString(10);
    var count = 10;
    var latch = new CountDownLatch(count);
    producer
        .send(
            IntStream.range(0, count)
                .mapToObj(i -> Record.builder().topic(topic).value(new byte[10]).build())
                .collect(Collectors.toUnmodifiableList()))
        .forEach(f -> f.whenComplete((m, e) -> latch.countDown()));

    latch.await();

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .config(
                ConsumerConfigs.ISOLATION_LEVEL_CONFIG,
                producer.transactional()
                    ? ConsumerConfigs.ISOLATION_LEVEL_COMMITTED
                    : ConsumerConfigs.ISOLATION_LEVEL_UNCOMMITTED)
            .build()) {
      Assertions.assertEquals(count, consumer.poll(count, Duration.ofSeconds(10)).size());
    }
  }

  private static Stream<Arguments> offerProducers() {
    return Stream.of(
        Arguments.of(
            Named.of(
                "normal producer",
                Producer.builder().bootstrapServers(bootstrapServers()).build())),
        Arguments.of(
            Named.of(
                "transactional producer",
                Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional())));
  }

  @Test
  void testSetTransactionIdManually() {
    try (var producer =
        Producer.builder()
            .bootstrapServers(bootstrapServers())
            .config(ProducerConfigs.TRANSACTIONAL_ID_CONFIG, "chia")
            .build()) {
      Assertions.assertTrue(producer.transactional());
      Assertions.assertTrue(producer.transactionId().isPresent());
    }
  }

  @Test
  void testClientId() {
    var clientId = Utils.randomString();
    try (var producer =
        Producer.builder()
            .bootstrapServers(bootstrapServers())
            .config(ProducerConfigs.CLIENT_ID_CONFIG, clientId)
            .build()) {
      Assertions.assertEquals(clientId, producer.clientId());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        ProducerConfigs.COMPRESSION_TYPE_NONE,
        ProducerConfigs.COMPRESSION_TYPE_GZIP,
        ProducerConfigs.COMPRESSION_TYPE_LZ4,
        ProducerConfigs.COMPRESSION_TYPE_SNAPPY,
        ProducerConfigs.COMPRESSION_TYPE_ZSTD
      })
  void testCompression(String compression) {
    try (var producer =
        Producer.builder()
            .bootstrapServers(bootstrapServers())
            .config(ProducerConfigs.COMPRESSION_TYPE_NONE, compression)
            .build()) {
      producer
          .send(Record.builder().topic(Utils.randomString()).key(new byte[10]).build())
          .toCompletableFuture()
          .join();
    }
  }
}
