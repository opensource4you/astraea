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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.consumer.Header;
import org.astraea.common.consumer.Isolation;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class ProducerTest extends RequireBrokerCluster {

  @Test
  void testSender() throws ExecutionException, InterruptedException {
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
              .sender()
              .topic(topicName)
              .key(key)
              .timestamp(timestamp)
              .headers(List.of(header))
              .run()
              .toCompletableFuture()
              .get();
      Assertions.assertEquals(topicName, metadata.topic());
      Assertions.assertEquals(timestamp, metadata.timestamp());
    }

    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .fromBeginning()
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
      var senders = new ArrayList<Sender<String, byte[]>>(3);
      while (senders.size() < 3) {
        senders.add(
            producer
                .sender()
                .topic(topicName)
                .key(key)
                .timestamp(timestamp)
                .headers(List.of(header)));
      }
      producer.send(senders);
    }

    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .fromBeginning()
            .keyDeserializer(Deserializer.STRING)
            .isolation(Isolation.READ_COMMITTED)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(10));
      Assertions.assertEquals(3, records.size());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void testInvalidSender() {
    try (var producer =
        Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> producer.send(List.of((Sender<byte[], byte[]>) Mockito.mock(Sender.class))));
    }
  }

  @ParameterizedTest
  @MethodSource("offerProducers")
  void testSingleSend(Producer<byte[], byte[]> producer)
      throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);

    producer.sender().topic(topic).value(new byte[10]).run().toCompletableFuture().get();

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .fromBeginning()
            .isolation(
                producer.transactional() ? Isolation.READ_COMMITTED : Isolation.READ_UNCOMMITTED)
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
                .mapToObj(i -> producer.sender().topic(topic).value(new byte[10]))
                .collect(Collectors.toUnmodifiableList()))
        .forEach(f -> f.whenComplete((m, e) -> latch.countDown()));

    latch.await();

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .fromBeginning()
            .isolation(
                producer.transactional() ? Isolation.READ_COMMITTED : Isolation.READ_UNCOMMITTED)
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
        Producer.builder().bootstrapServers(bootstrapServers()).transactionId("chia").build()) {
      Assertions.assertTrue(producer.transactional());
      Assertions.assertTrue(producer.transactionId().isPresent());
    }
  }

  @Test
  void testClientId() {
    var clientId = Utils.randomString();
    try (var producer =
        Producer.builder().bootstrapServers(bootstrapServers()).clientId(clientId).build()) {
      Assertions.assertEquals(clientId, producer.clientId());
    }
  }
}
