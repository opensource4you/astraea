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
package org.astraea.common.consumer;

import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.astraea.common.consumer.SeekStrategy.DISTANCE_FROM_BEGINNING;
import static org.astraea.common.consumer.SeekStrategy.DISTANCE_FROM_LATEST;
import static org.astraea.common.consumer.SeekStrategy.SEEK_TO;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.errors.WakeupException;
import org.astraea.common.FutureUtils;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.assignor.RandomAssignor;
import org.astraea.common.producer.Producer;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ConsumerTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  private static void produceData(String topic, int size) {
    try (var producer = Producer.builder().bootstrapServers(SERVICE.bootstrapServers()).build()) {
      IntStream.range(0, size)
          .forEach(
              i ->
                  producer.send(
                      org.astraea.common.producer.Record.builder()
                          .topic(topic)
                          .key(String.valueOf(i).getBytes(StandardCharsets.UTF_8))
                          .value(Utils.randomString().getBytes(StandardCharsets.UTF_8))
                          .build()));
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
            .bootstrapServers(SERVICE.bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
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
            .bootstrapServers(SERVICE.bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG, ConsumerConfigs.AUTO_OFFSET_RESET_LATEST)
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
            .bootstrapServers(SERVICE.bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG, ConsumerConfigs.AUTO_OFFSET_RESET_LATEST)
            .build()) {
      var service = Executors.newSingleThreadExecutor();
      service.execute(
          () -> {
            Utils.sleep(Duration.ofSeconds(3));
            consumer.wakeup();
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
                  .bootstrapServers(SERVICE.bootstrapServers())
                  .config(
                      ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                      ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
                  .config(ConsumerConfigs.GROUP_ID_CONFIG, id)
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
            .bootstrapServers(SERVICE.bootstrapServers())
            .config(ConsumerConfigs.GROUP_INSTANCE_ID_CONFIG, staticId)
            .build()) {
      Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(2)).size());
      Assertions.assertEquals(staticId, consumer.groupInstanceId().get());
    }
  }

  @Test
  void testDistanceFromLatest() {
    var count = 10;
    var topic = Utils.randomString(10);
    try (var producer = Producer.of(SERVICE.bootstrapServers())) {
      IntStream.range(0, count)
          .forEach(
              i ->
                  producer.send(
                      org.astraea.common.producer.Record.builder()
                          .topic(topic)
                          .value(String.valueOf(count).getBytes(StandardCharsets.UTF_8))
                          .build()));
      producer.flush();
    }
    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(SERVICE.bootstrapServers())
            .seek(DISTANCE_FROM_LATEST, 3)
            .build()) {
      Assertions.assertEquals(3, consumer.poll(4, Duration.ofSeconds(5)).size());
    }

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(SERVICE.bootstrapServers())
            .seek(DISTANCE_FROM_LATEST, 1000)
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
            .bootstrapServers(SERVICE.bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .build()) {

      // poll() returns immediately, if there is(/are) record(s) to poll.
      produceData(topic, count);
      Assertions.assertTimeout(
          Duration.ofSeconds(10), () -> consumer.poll(Duration.ofSeconds(Integer.MAX_VALUE)));
    }
  }

  @Test
  void testAssignment() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var producer = Producer.of(SERVICE.bootstrapServers())) {
      var partitionNum = 2;
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitionNum)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
        for (int recordIdx = 0; recordIdx < 10; recordIdx++) {
          producer.send(
              org.astraea.common.producer.Record.builder()
                  .topic(topic)
                  .partition(partitionId)
                  .value(ByteBuffer.allocate(4).putInt(recordIdx).array())
                  .build());
        }
      }
      producer.flush();
    }

    try (var consumer =
        Consumer.forPartitions(Set.of(TopicPartition.of(topic, "1")))
            .bootstrapServers(SERVICE.bootstrapServers())
            .seek(DISTANCE_FROM_LATEST, 20)
            .build()) {
      var records = consumer.poll(20, Duration.ofSeconds(5));
      Assertions.assertEquals(10, records.size());
      Assertions.assertEquals(
          nCopies(10, 1), records.stream().map(Record::partition).collect(toList()));
    }

    try (var consumer =
        Consumer.forPartitions(Set.of(TopicPartition.of(topic, "0"), TopicPartition.of(topic, "1")))
            .bootstrapServers(SERVICE.bootstrapServers())
            .seek(DISTANCE_FROM_LATEST, 20)
            .build()) {
      var records = consumer.poll(20, Duration.ofSeconds(5));
      Assertions.assertEquals(20, records.size());
      Assertions.assertEquals(
          Stream.concat(nCopies(10, 0).stream(), nCopies(10, 1).stream()).collect(toList()),
          records.stream().map(Record::partition).sorted().collect(toList()));
    }
  }

  @Test
  void testCommitOffset() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var producer = Producer.of(SERVICE.bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      producer.send(
          org.astraea.common.producer.Record.builder().topic(topic).value(new byte[10]).build());
      producer.flush();

      var groupId = Utils.randomString(10);
      try (var consumer =
          Consumer.forTopics(Set.of(topic))
              .config(ConsumerConfigs.GROUP_ID_CONFIG, groupId)
              .bootstrapServers(SERVICE.bootstrapServers())
              .config(
                  ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                  ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
              .config(ConsumerConfigs.ENABLE_AUTO_COMMIT_CONFIG, "false")
              .build()) {
        Assertions.assertEquals(1, consumer.poll(1, Duration.ofSeconds(4)).size());
        Assertions.assertEquals(
            1, admin.consumerGroups(Set.of(groupId)).toCompletableFuture().join().size());
        // no offsets are committed, so there is no progress.
        Assertions.assertEquals(
            0,
            admin
                .consumerGroups(Set.of(groupId))
                .toCompletableFuture()
                .join()
                .iterator()
                .next()
                .consumeProgress()
                .size());

        // commit offsets manually, so we can "see" the progress now.
        consumer.commitOffsets(Duration.ofSeconds(3));
        Assertions.assertEquals(
            1, admin.consumerGroups(Set.of(groupId)).toCompletableFuture().join().size());
        Assertions.assertEquals(
            1,
            admin
                .consumerGroups(Set.of(groupId))
                .toCompletableFuture()
                .join()
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
                  .bootstrapServers(SERVICE.bootstrapServers())
                  .seek(DISTANCE_FROM_BEGINNING, distanceFromBeginning)
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
                  .bootstrapServers(SERVICE.bootstrapServers())
                  .seek(SEEK_TO, seekTo)
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
                .bootstrapServers(SERVICE.bootstrapServers())
                .seek(SEEK_TO, -1)
                .build(),
        "seek value should >= 0");
  }

  @Test
  void testResubscribeTopics() {
    var topic = Utils.randomString(10);
    produceData(topic, 100);
    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(SERVICE.bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .build()) {
      Assertions.assertNotEquals(0, consumer.poll(Duration.ofSeconds(5)).size());
      consumer.unsubscribe();
      Assertions.assertThrows(
          IllegalStateException.class, () -> consumer.poll(Duration.ofSeconds(2)));
      // unsubscribe is idempotent op
      consumer.unsubscribe();
      consumer.unsubscribe();
      consumer.unsubscribe();

      consumer.resubscribe();
      Assertions.assertNotEquals(0, consumer.poll(Duration.ofSeconds(5)).size());

      // resubscribe is idempotent op
      consumer.resubscribe();
      consumer.resubscribe();
      consumer.resubscribe();
    }
  }

  @Test
  void testResubscribePartitions() {
    var topic = Utils.randomString(10);
    produceData(topic, 100);
    try (var consumer =
        Consumer.forPartitions(Set.of(TopicPartition.of(topic, 0)))
            .bootstrapServers(SERVICE.bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .build()) {
      Assertions.assertNotEquals(0, consumer.poll(Duration.ofSeconds(5)).size());
      consumer.unsubscribe();
      Assertions.assertThrows(
          IllegalStateException.class, () -> consumer.poll(Duration.ofSeconds(2)));
      // unsubscribe is idempotent op
      consumer.unsubscribe();
      consumer.unsubscribe();
      consumer.unsubscribe();

      consumer.resubscribe();
      Assertions.assertNotEquals(0, consumer.poll(Duration.ofSeconds(5)).size());

      // resubscribe is idempotent op
      consumer.resubscribe();
      consumer.resubscribe();
      consumer.resubscribe();
    }
  }

  @Test
  void testCreateConsumersConcurrent() {
    var partitions = 3;
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitions)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(3));
    }

    // one consume is idle
    var groupId = Utils.randomString(10);
    var consumers = partitions + 1;
    var log = new ConcurrentHashMap<Integer, Integer>();
    var closed = new AtomicBoolean(false);
    var fs =
        FutureUtils.sequence(
            IntStream.range(0, consumers)
                .mapToObj(
                    index ->
                        CompletableFuture.runAsync(
                            () -> {
                              try (var consumer =
                                  Consumer.forTopics(Set.of(topic))
                                      .config(ConsumerConfigs.GROUP_ID_CONFIG, groupId)
                                      .bootstrapServers(SERVICE.bootstrapServers())
                                      .seek(SEEK_TO, 0)
                                      .consumerRebalanceListener(ps -> log.put(index, ps.size()))
                                      .build()) {
                                while (!closed.get()) consumer.poll(Duration.ofSeconds(2));
                              }
                            }))
                .collect(Collectors.toUnmodifiableList()));
    Utils.waitFor(() -> log.size() == consumers, Duration.ofSeconds(15));
    Utils.waitFor(
        () -> log.values().stream().filter(ps -> ps == 0).count() == 1, Duration.ofSeconds(15));
    closed.set(true);
    fs.join();
  }

  @Test
  void testClientId() {
    var topic = Utils.randomString(10);
    var clientId0 = Utils.randomString();
    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(SERVICE.bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .config(ConsumerConfigs.CLIENT_ID_CONFIG, clientId0)
            .build()) {
      Assertions.assertEquals(clientId0, consumer.clientId());
    }

    var clientId1 = Utils.randomString();
    try (var consumer =
        Consumer.forPartitions(Set.of(TopicPartition.of(topic, 0)))
            .bootstrapServers(SERVICE.bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .config(ConsumerConfigs.CLIENT_ID_CONFIG, clientId1)
            .build()) {
      Assertions.assertEquals(clientId1, consumer.clientId());
    }
  }

  @Timeout(20)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testIterator(boolean isAssigned) {
    var records = 100;
    var topic = Utils.randomString();
    produceData(topic, records);

    Function<IteratorLimit<byte[], byte[]>, Stream<Record<byte[], byte[]>>> supplier =
        limit -> {
          var iter =
              isAssigned
                  ? Consumer.forPartitions(Set.of(TopicPartition.of(topic, 0)))
                      .bootstrapServers(SERVICE.bootstrapServers())
                      .seek(DISTANCE_FROM_BEGINNING, 0)
                      .iterator(List.of(limit))
                  : Consumer.forTopics(Set.of(topic))
                      .bootstrapServers(SERVICE.bootstrapServers())
                      .config(
                          ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                          ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
                      .iterator(List.of(limit));
          ;
          return StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false);
        };

    // test count limit
    Assertions.assertEquals(records, supplier.apply(IteratorLimit.count(records)).count());

    // test idle limit
    Assertions.assertEquals(
        records, supplier.apply(IteratorLimit.idle(Duration.ofSeconds(3))).count());

    // test size limit
    Assertions.assertEquals(records, supplier.apply(IteratorLimit.size(1L)).count());

    // test elapsed time limit
    Assertions.assertEquals(
        records, supplier.apply(IteratorLimit.elapsed(Duration.ofSeconds(3))).count());
  }

  @Test
  void testRandomAssignorWithSingleConsumer() {
    var topic = "testAssignor";
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var producer = Producer.of(SERVICE.bootstrapServers())) {
      var partitionNum = 3;
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitionNum)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(3));

      for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
        for (int recordIdx = 0; recordIdx < 10; recordIdx++) {
          producer.send(
              org.astraea.common.producer.Record.builder()
                  .topic(topic)
                  .partition(partitionId)
                  .value(ByteBuffer.allocate(4).putInt(recordIdx).array())
                  .build());
        }
      }
      producer.flush();
    }

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(SERVICE.bootstrapServers())
            .config(
                ConsumerConfigs.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                RandomAssignor.class.getName())
            .seek(DISTANCE_FROM_BEGINNING, 0)
            .build()) {
      var records = consumer.poll(30, Duration.ofSeconds(5));
      Assertions.assertEquals(30, records.size());
      Assertions.assertEquals(3, consumer.assignments().size());
    }
  }

  @Test
  void testRandomAssignorWithTwoTopicsAndMultipleConsumers() {
    var topic1 = "test1";
    var topic2 = "test2";
    var topics = Set.of(topic1, topic2);
    var partitions = 0;
    var random = new Random();
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var producer = Producer.of(SERVICE.bootstrapServers())) {
      var partitionNum = random.nextInt(15) + 1;
      admin
          .creator()
          .topic(topic1)
          .numberOfPartitions(partitionNum)
          .run()
          .toCompletableFuture()
          .join();
      admin
          .creator()
          .topic(topic2)
          .numberOfPartitions(partitionNum)
          .run()
          .toCompletableFuture()
          .join();
      partitions = 2 * partitionNum;
      Utils.sleep(Duration.ofSeconds(6));
      for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
        for (int recordIdx = 0; recordIdx < 10; recordIdx++) {
          for (var topic : topics) {
            producer.send(
                org.astraea.common.producer.Record.builder()
                    .topic(topic)
                    .partition(partitionId)
                    .value(ByteBuffer.allocate(4).putInt(recordIdx).array())
                    .build());
          }
        }
      }
      producer.flush();
    }
    var consumers = 3;
    var groupId = "astraea";
    var closed = new AtomicBoolean(false);
    var assignments = new ConcurrentHashMap<Integer, Integer>();
    var totalPartitions = new AtomicInteger();
    var latches = new CountDownLatch(consumers);
    var futures =
        FutureUtils.sequence(
            IntStream.range(0, consumers)
                .mapToObj(
                    index ->
                        CompletableFuture.runAsync(
                            () -> {
                              try (var consumer =
                                  Consumer.forTopics(topics)
                                      .config(ConsumerConfigs.GROUP_ID_CONFIG, groupId)
                                      .config(
                                          ConsumerConfigs.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                                          RandomAssignor.class.getName())
                                      .bootstrapServers(SERVICE.bootstrapServers())
                                      .seek(SEEK_TO, 0)
                                      .consumerRebalanceListener(
                                          ps -> {
                                            assignments.put(index, ps.size());
                                            latches.countDown();
                                          })
                                      .build()) {
                                while (!closed.get()) consumer.poll(Duration.ofSeconds(2));
                              }
                            }))
                .collect(Collectors.toUnmodifiableList()));
    Utils.waitFor(() -> latches.getCount() == 0, Duration.ofSeconds(10));
    assignments.values().forEach(v -> totalPartitions.set(totalPartitions.get() + v));

    Assertions.assertEquals(partitions, totalPartitions.get());
    closed.set(true);
    futures.join();
  }
}
