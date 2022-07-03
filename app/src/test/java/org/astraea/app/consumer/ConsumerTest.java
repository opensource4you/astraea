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
package org.astraea.app.consumer;

import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.astraea.app.consumer.Builder.SeekStrategy.DISTANCE_FROM_BEGINNING;
import static org.astraea.app.consumer.Builder.SeekStrategy.DISTANCE_FROM_LATEST;
import static org.astraea.app.consumer.Builder.SeekStrategy.SEEK_TO;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.common.errors.WakeupException;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.common.Utils;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ConsumerTest extends RequireBrokerCluster {

  private static void produceData(String topic, int size) {
    try (var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      IntStream.range(0, size)
          .forEach(
              i ->
                  producer
                      .sender()
                      .topic(topic)
                      .key(String.valueOf(i).getBytes(StandardCharsets.UTF_8))
                      .run());
      producer.flush();
    }
  }

  @Test
  void testFromBeginning() {
    var recordCount = 100;
    var topic = "testPoll";
    produceData(topic, recordCount);
    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .fromBeginning()
            .build()) {

      Assertions.assertEquals(
          recordCount, consumer.poll(recordCount, Duration.ofSeconds(10)).size());
    }
  }

  @Test
  void testFromLatest() {
    var topic = "testFromLatest";
    produceData(topic, 1);
    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .fromLatest()
            .build()) {

      Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
    }
  }

  @Timeout(7)
  @Test
  void testWakeup() throws InterruptedException {
    var topic = "testWakeup";
    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .fromLatest()
            .build()) {
      var service = Executors.newSingleThreadExecutor();
      service.execute(
          () -> {
            try {
              TimeUnit.SECONDS.sleep(3);
              consumer.wakeup();
            } catch (InterruptedException ignored) {
              // swallow
            }
          });
      // this call will be broken after 3 seconds
      Assertions.assertThrows(WakeupException.class, () -> consumer.poll(Duration.ofSeconds(100)));

      service.shutdownNow();
      Assertions.assertTrue(service.awaitTermination(3, TimeUnit.SECONDS));
    }
  }

  @Test
  void testGroupId() {
    var groupId = Utils.randomString(10);
    var topic = Utils.randomString(10);
    produceData(topic, 1);

    java.util.function.BiConsumer<String, Integer> testConsumer =
        (id, expectedSize) -> {
          try (var consumer =
              Consumer.forTopics(Set.of(topic))
                  .bootstrapServers(bootstrapServers())
                  .fromBeginning()
                  .groupId(id)
                  .build()) {
            Assertions.assertEquals(
                expectedSize, consumer.poll(expectedSize, Duration.ofSeconds(5)).size());
            Assertions.assertEquals(id, consumer.groupId());
            Assertions.assertNotNull(consumer.memberId());
            Assertions.assertFalse(consumer.groupInstanceId().isPresent());
          }
        };

    testConsumer.accept(groupId, 1);

    // the data is fetched already, so it should not return any data
    testConsumer.accept(groupId, 0);

    // use different group id
    testConsumer.accept("another_group", 0);
  }

  @Test
  void testGroupInstanceId() {
    var staticId = Utils.randomString(10);
    try (var consumer =
        Consumer.forTopics(Set.of(Utils.randomString(10)))
            .bootstrapServers(bootstrapServers())
            .groupInstanceId(staticId)
            .build()) {
      Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(2)).size());
      Assertions.assertEquals(staticId, consumer.groupInstanceId().get());
    }
  }

  @Test
  void testDistanceFromLatest() {
    var count = 10;
    var topic = Utils.randomString(10);
    try (var producer = Producer.of(bootstrapServers())) {
      IntStream.range(0, count)
          .forEach(
              i ->
                  producer
                      .sender()
                      .topic(topic)
                      .value(String.valueOf(count).getBytes(StandardCharsets.UTF_8))
                      .run());
      producer.flush();
    }
    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .seekStrategy(DISTANCE_FROM_LATEST, 3)
            .build()) {
      Assertions.assertEquals(3, consumer.poll(4, Duration.ofSeconds(5)).size());
    }

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .seekStrategy(DISTANCE_FROM_LATEST, 1000)
            .build()) {
      Assertions.assertEquals(10, consumer.poll(11, Duration.ofSeconds(5)).size());
    }
  }

  @Test
  void testRecordsPollingTime() {
    var count = 1;
    var topic = "testPollingTime";
    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .fromBeginning()
            .build()) {

      // poll() returns immediately, if there is(/are) record(s) to poll.
      produceData(topic, count);
      Assertions.assertTimeout(
          Duration.ofSeconds(10), () -> consumer.poll(Duration.ofSeconds(Integer.MAX_VALUE)));
    }
  }

  @Test
  void testAssignment() throws InterruptedException {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      var partitionNum = 2;
      admin.creator().topic(topic).numberOfPartitions(partitionNum).create();
      TimeUnit.SECONDS.sleep(2);

      for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
        for (int recordIdx = 0; recordIdx < 10; recordIdx++) {
          producer
              .sender()
              .topic(topic)
              .partition(partitionId)
              .value(ByteBuffer.allocate(4).putInt(recordIdx).array())
              .run();
        }
      }
      producer.flush();
    }

    try (var consumer =
        Consumer.forPartitions(Set.of(TopicPartition.of(topic, "1")))
            .bootstrapServers(bootstrapServers())
            .seekStrategy(DISTANCE_FROM_LATEST, 20)
            .build()) {
      var records = consumer.poll(20, Duration.ofSeconds(5));
      Assertions.assertEquals(10, records.size());
      Assertions.assertEquals(
          nCopies(10, 1), records.stream().map(Record::partition).collect(toList()));
    }

    try (var consumer =
        Consumer.forPartitions(Set.of(TopicPartition.of(topic, "0"), TopicPartition.of(topic, "1")))
            .bootstrapServers(bootstrapServers())
            .seekStrategy(DISTANCE_FROM_LATEST, 20)
            .build()) {
      var records = consumer.poll(20, Duration.ofSeconds(5));
      Assertions.assertEquals(20, records.size());
      Assertions.assertEquals(
          Stream.concat(nCopies(10, 0).stream(), nCopies(10, 1).stream()).collect(toList()),
          records.stream().map(Record::partition).sorted().collect(toList()));
    }
  }

  @Test
  void testCommitOffset() throws InterruptedException {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).create();
      TimeUnit.SECONDS.sleep(2);
      producer.sender().topic(topic).value(new byte[10]).run();
      producer.flush();

      var groupId = Utils.randomString(10);
      try (var consumer =
          Consumer.forTopics(Set.of(topic))
              .groupId(groupId)
              .bootstrapServers(bootstrapServers())
              .fromBeginning()
              .disableAutoCommitOffsets()
              .build()) {
        Assertions.assertEquals(1, consumer.poll(1, Duration.ofSeconds(4)).size());
        Assertions.assertEquals(1, admin.consumerGroups(Set.of(groupId)).size());
        // no offsets are committed, so there is no progress.
        Assertions.assertEquals(
            0,
            admin
                .consumerGroups(Set.of(groupId))
                .values()
                .iterator()
                .next()
                .consumeProgress()
                .size());

        // commit offsets manually, so we can "see" the progress now.
        consumer.commitOffsets(Duration.ofSeconds(3));
        Assertions.assertEquals(1, admin.consumerGroups(Set.of(groupId)).size());
        Assertions.assertEquals(
            1,
            admin
                .consumerGroups(Set.of(groupId))
                .values()
                .iterator()
                .next()
                .consumeProgress()
                .size());
      }
    }
  }

  @Test
  void testDistanceFromBeginning() {
    var topic = Utils.randomString(10);
    produceData(topic, 10);

    BiConsumer<Integer, Integer> internalTest =
        (distanceFromBeginning, expectedSize) -> {
          try (var consumer =
              Consumer.forTopics(Set.of(topic))
                  .bootstrapServers(bootstrapServers())
                  .seekStrategy(DISTANCE_FROM_BEGINNING, distanceFromBeginning)
                  .build()) {
            Assertions.assertEquals(expectedSize, consumer.poll(10, Duration.ofSeconds(5)).size());
          }
        };

    internalTest.accept(3, 7);
    internalTest.accept(1000, 0);
  }

  @Test
  void testSeekTo() {
    var topic = Utils.randomString(10);
    produceData(topic, 10);

    BiConsumer<Integer, Integer> internalTest =
        (seekTo, expectedSize) -> {
          try (var consumer =
              Consumer.forTopics(Set.of(topic))
                  .bootstrapServers(bootstrapServers())
                  .seekStrategy(SEEK_TO, seekTo)
                  .build()) {
            Assertions.assertEquals(expectedSize, consumer.poll(10, Duration.ofSeconds(5)).size());
          }
        };

    internalTest.accept(9, 1);
    internalTest.accept(1000, 0);
  }

  @Test
  void testInvalidSeekValue() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Consumer.forTopics(Set.of("test"))
                .bootstrapServers(bootstrapServers())
                .seekStrategy(SEEK_TO, -1)
                .build(),
        "seek value should >= 0");
  }
}
