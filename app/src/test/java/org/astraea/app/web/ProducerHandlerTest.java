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
package org.astraea.app.web;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProducerHandlerTest extends RequireBrokerCluster {

  @Test
  void testListProducers() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      var handler = new ProducerHandler(admin);
      producer
          .send(Record.builder().topic(topicName).value(new byte[1]).build())
          .toCompletableFuture()
          .join();

      var result =
          Assertions.assertInstanceOf(
              ProducerHandler.Partitions.class,
              handler.get(Channel.EMPTY).toCompletableFuture().join());
      Assertions.assertNotEquals(0, result.partitions.size());

      var partitions =
          result.partitions.stream()
              .filter(t -> t.topic.equals(topicName))
              .collect(Collectors.toUnmodifiableList());
      Assertions.assertEquals(1, partitions.size());
      Assertions.assertEquals(topicName, partitions.iterator().next().topic);
      Assertions.assertEquals(0, partitions.iterator().next().partition);
    }
  }

  @Test
  void testQuerySinglePartition() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(2).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      var handler = new ProducerHandler(admin);
      producer
          .send(Record.builder().topic(topicName).partition(0).value(new byte[1]).build())
          .toCompletableFuture()
          .join();
      producer
          .send(Record.builder().topic(topicName).partition(1).value(new byte[1]).build())
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      Assertions.assertEquals(
          1,
          handler
              .partitions(
                  Map.of(ProducerHandler.TOPIC_KEY, topicName, ProducerHandler.PARTITION_KEY, "0"))
              .toCompletableFuture()
              .join()
              .size());

      var result0 =
          Assertions.assertInstanceOf(
              ProducerHandler.Partitions.class,
              handler
                  .get(
                      Channel.ofQueries(
                          Map.of(
                              ProducerHandler.TOPIC_KEY,
                              topicName,
                              ProducerHandler.PARTITION_KEY,
                              "0")))
                  .toCompletableFuture()
                  .join());
      Assertions.assertEquals(1, result0.partitions.size());
      Assertions.assertEquals(topicName, result0.partitions.iterator().next().topic);
      Assertions.assertEquals(0, result0.partitions.iterator().next().partition);

      Assertions.assertEquals(
          2,
          handler
              .partitions(Map.of(ProducerHandler.TOPIC_KEY, topicName))
              .toCompletableFuture()
              .join()
              .size());

      var result1 =
          Assertions.assertInstanceOf(
              ProducerHandler.Partitions.class,
              handler
                  .get(Channel.ofQueries(Map.of(ProducerHandler.TOPIC_KEY, topicName)))
                  .toCompletableFuture()
                  .join());
      Assertions.assertEquals(2, result1.partitions.size());
      Assertions.assertEquals(topicName, result1.partitions.iterator().next().topic);
      Assertions.assertEquals(
          Set.of(0, 1),
          result1.partitions.stream().map(p -> p.partition).collect(Collectors.toSet()));
    }
  }

  @Test
  void testPartitions() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new ProducerHandler(admin);
      var topics = admin.topicNames(false).toCompletableFuture().join();
      Assertions.assertEquals(
          admin.topicPartitions(topics).toCompletableFuture().join(),
          handler.partitions(Map.of()).toCompletableFuture().join());
      var target = admin.topicPartitions(topics).toCompletableFuture().join().iterator().next();
      Assertions.assertEquals(
          Set.of(target),
          handler
              .partitions(
                  Map.of(
                      ProducerHandler.TOPIC_KEY,
                      target.topic(),
                      ProducerHandler.PARTITION_KEY,
                      String.valueOf(target.partition())))
              .toCompletableFuture()
              .join());

      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler.partitions(
                  Map.of(
                      ProducerHandler.TOPIC_KEY,
                      target.topic(),
                      ProducerHandler.PARTITION_KEY,
                      "a")));

      admin.creator().topic(topicName).numberOfPartitions(3).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));

      Assertions.assertEquals(
          3,
          handler
              .partitions(Map.of(ProducerHandler.TOPIC_KEY, topicName))
              .toCompletableFuture()
              .join()
              .size());
    }
  }
}
