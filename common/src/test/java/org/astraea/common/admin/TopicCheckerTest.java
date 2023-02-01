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
package org.astraea.common.admin;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopicCheckerTest {

  private final Service service = Service.builder().numberOfBrokers(3).build();

  @AfterEach
  void closeService() {
    service.close();
  }

  @Test
  void testNoWrite() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(service.bootstrapServers())) {
      admin.creator().topic(topic).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));

      Assertions.assertEquals(
          Set.of(topic),
          admin
              .topicNames(
                  List.of(TopicChecker.noWriteAfter(Duration.ofSeconds(3), Duration.ofSeconds(3))))
              .toCompletableFuture()
              .join());

      try (var producer = Producer.builder().bootstrapServers(service.bootstrapServers()).build()) {
        producer
            .send(Record.builder().topic(topic).value("1".getBytes()).build())
            .toCompletableFuture()
            .join();
      }
      Assertions.assertEquals(
          Set.of(),
          admin
              .topicNames(
                  List.of(TopicChecker.noWriteAfter(Duration.ofSeconds(3), Duration.ofSeconds(3))))
              .toCompletableFuture()
              .join());
    }
  }

  @Test
  void testNoConsumerGroup() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(service.bootstrapServers())) {
      admin.creator().topic(topic).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));

      Assertions.assertEquals(
          Set.of(topic),
          admin.topicNames(List.of(TopicChecker.NO_CONSUMER_GROUP)).toCompletableFuture().join());

      try (var consumer =
          Consumer.forTopics(Set.of(topic)).bootstrapServers(service.bootstrapServers()).build()) {
        consumer.poll(Duration.ofSeconds(5));
        Assertions.assertEquals(
            Set.of(),
            admin.topicNames(List.of(TopicChecker.NO_CONSUMER_GROUP)).toCompletableFuture().join());
      }
    }
  }

  @Test
  void testNoData() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(service.bootstrapServers())) {
      admin.creator().topic(topic).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));

      Assertions.assertEquals(
          Set.of(topic),
          admin.topicNames(List.of(TopicChecker.NO_DATA)).toCompletableFuture().join());

      try (var producer = Producer.builder().bootstrapServers(service.bootstrapServers()).build()) {
        producer
            .send(Record.builder().topic(topic).value("1".getBytes()).build())
            .toCompletableFuture()
            .join();
      }

      Assertions.assertEquals(
          Set.of(), admin.topicNames(List.of(TopicChecker.NO_DATA)).toCompletableFuture().join());
    }
  }

  @Test
  void testSkewPartition() {
    var singlePartitionTopic = Utils.randomString();
    var topic = Utils.randomString();
    try (var admin = Admin.of(service.bootstrapServers())) {
      admin
          .creator()
          .topic(singlePartitionTopic)
          .numberOfPartitions(1)
          .run()
          .toCompletableFuture()
          .join();
      admin.creator().topic(topic).numberOfPartitions(2).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));

      Assertions.assertEquals(
          Set.of(),
          admin.topicNames(List.of(TopicChecker.skewPartition(0.5))).toCompletableFuture().join());

      try (var producer = Producer.builder().bootstrapServers(service.bootstrapServers()).build()) {
        IntStream.range(0, 100)
            .forEach(
                ignored ->
                    producer.send(
                        List.of(
                            Record.builder()
                                .topic(singlePartitionTopic)
                                .value("1".getBytes())
                                .partition(0)
                                .build(),
                            Record.builder()
                                .topic(topic)
                                .value("1".getBytes())
                                .partition(0)
                                .build())));
        producer.flush();
      }

      Assertions.assertEquals(
          Set.of(topic),
          admin.topicNames(List.of(TopicChecker.skewPartition(0.5))).toCompletableFuture().join());
    }
  }
}
