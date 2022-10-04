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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.astraea.common.DataRate;
import org.astraea.common.DataSize;
import org.astraea.common.ExecutionRuntimeException;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Serializer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AdminTest extends RequireBrokerCluster {

  @Test
  void testCreator() {
    var topicName = "testCreator";
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topicName)
          .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"))
          .create();
      Utils.waitFor(
          () ->
              admin
                  .topics(Set.of(topicName))
                  .get(0)
                  .config()
                  .value(TopicConfig.COMPRESSION_TYPE_CONFIG)
                  .filter(value -> value.equals("lz4"))
                  .isPresent());

      var config = admin.topics(Set.of(topicName)).get(0).config();
      config.raw().keySet().forEach(key -> Assertions.assertTrue(config.value(key).isPresent()));
      Assertions.assertTrue(config.raw().containsValue("lz4"));
    }
  }

  @Test
  void testCreateTopicRepeatedly() {
    var topicName = "testCreateTopicRepeatedly";
    try (var admin = Admin.of(bootstrapServers())) {
      Runnable createTopic =
          () ->
              admin
                  .creator()
                  .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"))
                  .numberOfReplicas((short) 1)
                  .numberOfPartitions(3)
                  .topic(topicName)
                  .create();

      createTopic.run();
      Utils.waitFor(() -> admin.topicNames().contains(topicName));
      IntStream.range(0, 10).forEach(i -> createTopic.run());

      // changing number of partitions can producer error
      Assertions.assertInstanceOf(
          IllegalArgumentException.class,
          Assertions.assertThrows(
                  ExecutionException.class,
                  () ->
                      admin
                          .creator()
                          .numberOfPartitions(1)
                          .topic(topicName)
                          .run()
                          .toCompletableFuture()
                          .get())
              .getCause());

      // changing number of replicas can producer error
      Assertions.assertInstanceOf(
          IllegalArgumentException.class,
          Assertions.assertThrows(
                  ExecutionException.class,
                  () ->
                      admin
                          .creator()
                          .numberOfReplicas((short) 2)
                          .topic(topicName)
                          .run()
                          .toCompletableFuture()
                          .get())
              .getCause());

      // changing config can producer error
      Assertions.assertInstanceOf(
          IllegalArgumentException.class,
          Assertions.assertThrows(
                  ExecutionException.class,
                  () ->
                      admin
                          .creator()
                          .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"))
                          .topic(topicName)
                          .run()
                          .toCompletableFuture()
                          .get())
              .getCause());
    }
  }

  @Test
  void testPartitions() {
    var topicName = "testPartitions";
    try (var admin = Admin.of(bootstrapServers())) {
      var before = brokerIds().stream().mapToInt(id -> admin.topicPartitions(id).size()).sum();
      admin.creator().topic(topicName).numberOfPartitions(10).create();
      // wait for syncing topic creation
      Utils.sleep(Duration.ofSeconds(5));
      Assertions.assertTrue(admin.topicNames().contains(topicName));
      var partitions = admin.replicas(Set.of(topicName));
      Assertions.assertEquals(10, partitions.size());
      var logFolders =
          logFolders().values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
      partitions.forEach(
          replica ->
              Assertions.assertTrue(logFolders.stream().anyMatch(replica.dataFolder()::contains)));
      brokerIds().forEach(id -> Assertions.assertNotEquals(0, admin.topicPartitions(id).size()));
      var after = brokerIds().stream().mapToInt(id -> admin.topicPartitions(id).size()).sum();
      Assertions.assertEquals(before + 10, after);
    }
  }

  @Test
  void testOffsets() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).create();
      // wait for syncing topic creation
      Utils.sleep(Duration.ofSeconds(3));
      var partitions = admin.partitions(Set.of(topicName));
      Assertions.assertEquals(3, partitions.size());
      partitions.forEach(
          p -> {
            Assertions.assertEquals(0, p.earliestOffset());
            Assertions.assertEquals(0, p.latestOffset());
          });
      try (var producer = Producer.of(bootstrapServers())) {
        producer.sender().topic(topicName).key(new byte[100]).partition(0).run();
        producer.sender().topic(topicName).key(new byte[55]).partition(1).run();
        producer.sender().topic(topicName).key(new byte[33]).partition(2).run();
      }

      admin
          .partitions(Set.of(topicName))
          .forEach(p -> Assertions.assertEquals(0, p.earliestOffset()));
      admin
          .partitions(Set.of(topicName))
          .forEach(p -> Assertions.assertEquals(1, p.latestOffset()));
      admin
          .partitions(Set.of(topicName))
          .forEach(p -> Assertions.assertNotEquals(-1, p.maxTimestamp()));
    }
  }

  @Test
  void testConsumerGroups() {
    var topicName = "testConsumerGroups-Topic";
    var consumerGroup = "testConsumerGroups-Group";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).create();
      try (var c1 =
          Consumer.forTopics(Set.of(topicName))
              .bootstrapServers(bootstrapServers())
              .groupId(consumerGroup)
              .build()) {
        // wait for syncing topic creation
        Utils.sleep(Duration.ofSeconds(5));
        var consumerGroupMap = admin.consumerGroups(Set.of(consumerGroup));
        Assertions.assertEquals(1, consumerGroupMap.size());
        Assertions.assertTrue(
            consumerGroupMap.stream().anyMatch(cg -> cg.groupId().equals(consumerGroup)));

        try (var c2 =
            Consumer.forTopics(Set.of(topicName))
                .bootstrapServers(bootstrapServers())
                .groupId("abc")
                .build()) {
          var count =
              admin.consumerGroupIds().stream()
                  .mapToInt(t -> admin.consumerGroups(Set.of(t)).size())
                  .sum();
          Assertions.assertEquals(count, admin.consumerGroups(admin.consumerGroupIds()).size());
          Assertions.assertEquals(1, admin.consumerGroups(Set.of("abc")).size());
        }
      }
    }
  }

  @Test
  // There is a problem when migrating the log folder under Windows because the migrated source
  // cannot be deleted, so disabled this test on Windows for now.
  @DisabledOnOs(WINDOWS)
  void testMigrateSinglePartition() {
    var topicName = "testMigrateSinglePartition";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(1).create();
      // wait for syncing topic creation
      Utils.sleep(Duration.ofSeconds(5));
      var broker = admin.brokerIds().iterator().next();
      admin.migrator().partition(topicName, 0).moveTo(List.of(broker));
      Utils.waitFor(
          () -> {
            var partitionReplicas = admin.replicas(Set.of(topicName));
            return partitionReplicas.size() == 1
                && partitionReplicas.get(0).nodeInfo().id() == broker;
          });

      var currentBroker =
          admin.replicas(Set.of(topicName)).stream()
              .filter(replica -> replica.partition() == 0)
              .findFirst()
              .get()
              .nodeInfo()
              .id();
      var allPath = admin.brokerFolders();
      var otherPath =
          allPath.get(currentBroker).stream()
              .filter(
                  i ->
                      !i.contains(
                          admin.replicas(Set.of(topicName)).stream()
                              .filter(replica -> replica.partition() == 0)
                              .findFirst()
                              .get()
                              .dataFolder()))
              .collect(Collectors.toSet());
      admin
          .migrator()
          .partition(topicName, 0)
          .moveTo(Map.of(currentBroker, otherPath.iterator().next()));
      Utils.waitFor(
          () -> {
            var partitionReplicas = admin.replicas(Set.of(topicName));
            return partitionReplicas.size() == 1
                && partitionReplicas.get(0).dataFolder().equals(otherPath.iterator().next());
          });
    }
  }

  @Test
  void testDeclarePreferredLogDirectory() {
    // arrange
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = "DeclarePreferredLogDirectory_" + Utils.randomString();
      var topicPartition = TopicPartition.of(topic, 0);
      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 1).create();
      Utils.sleep(Duration.ofSeconds(1));
      var originalBroker =
          admin.replicas(Set.of(topic)).stream()
              .filter(replica -> replica.partition() == topicPartition.partition())
              .findFirst()
              .get()
              .nodeInfo()
              .id();
      var nextBroker = (originalBroker + 1) % brokerIds().size();
      var nextDir = logFolders().get(nextBroker).stream().findAny().orElseThrow();
      Supplier<Replica> replicaNow =
          () ->
              admin.replicas(Set.of(topic)).stream()
                  .filter(replica -> replica.partition() == topicPartition.partition())
                  .findFirst()
                  .get();

      // act, declare the preferred data directory
      Assertions.assertDoesNotThrow(
          () ->
              admin.migrator().partition(topic, 0).declarePreferredDir(Map.of(nextBroker, nextDir)),
          "The Migrator API should ignore the error");
      Utils.sleep(Duration.ofSeconds(1));

      // assert, nothing happened until the actual movement
      Assertions.assertNotEquals(nextBroker, replicaNow.get().nodeInfo().id());
      Assertions.assertNotEquals(nextDir, replicaNow.get().dataFolder());

      // act, perform the actual movement
      admin.migrator().partition(topic, 0).moveTo(List.of(nextBroker));
      Utils.sleep(Duration.ofSeconds(1));

      // assert, everything on the exact broker & dir
      Assertions.assertEquals(nextBroker, replicaNow.get().nodeInfo().id());
      Assertions.assertEquals(nextDir, replicaNow.get().dataFolder());
    }
  }

  @Test
  void testIllegalMigrationArgument() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      var topicPartition = TopicPartition.of(topic, 0);
      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 1).create();
      Utils.sleep(Duration.ofSeconds(1));
      var currentReplica =
          admin.replicas(Set.of(topic)).stream()
              .filter(replica -> replica.partition() == topicPartition.partition())
              .findFirst()
              .get();
      var currentBroker = currentReplica.nodeInfo().id();
      var notExistReplica = (currentBroker + 1) % brokerIds().size();
      var nextDir = logFolders().get(notExistReplica).iterator().next();

      Assertions.assertThrows(
          IllegalStateException.class,
          () -> admin.migrator().partition(topic, 0).moveTo(Map.of(notExistReplica, nextDir)));
    }
  }

  @Test
  void testIllegalPreferredDirArgument() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      var topicPartition = TopicPartition.of(topic, 0);
      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 1).create();
      Utils.sleep(Duration.ofSeconds(1));
      var currentReplica =
          admin.replicas(Set.of(topic)).stream()
              .filter(replica -> replica.partition() == topicPartition.partition())
              .findFirst()
              .get();
      var currentBroker = currentReplica.nodeInfo().id();
      var nextDir = logFolders().get(currentBroker).iterator().next();

      Assertions.assertThrows(
          IllegalStateException.class,
          () ->
              admin
                  .migrator()
                  .partition(topic, 0)
                  .declarePreferredDir(Map.of(currentBroker, nextDir)));
    }
  }

  @Test
  void testPartialMoveToArgument() {
    // arrange
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));
      var replica0 = 0;
      var replica1 = 1;
      var replica2 = 2;
      var folder0 = logFolders().get(replica0).iterator().next();
      var folder1 = logFolders().get(replica1).iterator().next();
      var folder2 = logFolders().get(replica2).iterator().next();

      // act, assert
      Assertions.assertDoesNotThrow(
          () -> admin.migrator().partition(topic, 0).moveTo(Map.of(replica0, folder0)));
      Assertions.assertDoesNotThrow(
          () ->
              admin
                  .migrator()
                  .partition(topic, 0)
                  .moveTo(Map.of(replica0, folder0, replica1, folder1)));
      Assertions.assertDoesNotThrow(
          () ->
              admin
                  .migrator()
                  .partition(topic, 0)
                  .moveTo(Map.of(replica0, folder0, replica1, folder1, replica2, folder2)));
    }
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void testMigrateAllPartitions() {
    var topicName = "testMigrateAllPartitions";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).create();
      // wait for syncing topic creation
      Utils.sleep(Duration.ofSeconds(5));
      var broker = admin.brokerIds().iterator().next();
      admin.migrator().topic(topicName).moveTo(List.of(broker));
      Utils.waitFor(
          () -> {
            var replicas = admin.replicas(Set.of(topicName));
            return replicas.stream().allMatch(r -> r.nodeInfo().id() == broker);
          });
    }
  }

  @Test
  void testReplicaSize() throws ExecutionException, InterruptedException {
    var topicName = "testReplicaSize";
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      producer.sender().topic(topicName).key(new byte[100]).run().toCompletableFuture().get();
      var originSize =
          admin.replicas(Set.of(topicName)).stream()
              .filter(replica -> replica.partition() == 0)
              .findFirst()
              .get()
              .size();

      // add data again
      producer.sender().topic(topicName).key(new byte[100]).run().toCompletableFuture().get();

      var newSize =
          admin.replicas(Set.of(topicName)).stream()
              .filter(replica -> replica.partition() == 0)
              .findFirst()
              .get()
              .size();
      Assertions.assertTrue(newSize > originSize);
    }
  }

  @Test
  void testCompact() {
    var topicName = "testCompacted";
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topicName)
          .configs(
              Map.of(
                  TopicCreator.MAX_COMPACTION_LAG_MS_CONFIG,
                  "1000",
                  TopicCreator.CLEANUP_POLICY_CONFIG,
                  TopicCreator.CLEANUP_POLICY_COMPACT))
          .create();

      var key = "key";
      var anotherKey = "anotherKey";
      var value = "value";
      try (var producer =
          Producer.builder()
              .keySerializer(Serializer.STRING)
              .valueSerializer(Serializer.STRING)
              .bootstrapServers(bootstrapServers())
              .build()) {
        IntStream.range(0, 10)
            .forEach(i -> producer.sender().key(key).value(value).topic(topicName).run());
        producer.flush();

        // sleep and produce more data to generate the new segment
        Utils.sleep(Duration.ofSeconds(2));
        IntStream.range(0, 10)
            .forEach(i -> producer.sender().key(anotherKey).value(value).topic(topicName).run());
        producer.flush();
      }

      // sleep for compact (the backoff of compact thread is reduced to 2 seconds.
      // see org.astraea.service.Services)
      Utils.sleep(Duration.ofSeconds(3));

      try (var consumer =
          Consumer.forTopics(Set.of(topicName))
              .keyDeserializer(Deserializer.STRING)
              .valueDeserializer(Deserializer.STRING)
              .fromBeginning()
              .bootstrapServers(bootstrapServers())
              .build()) {

        var records =
            IntStream.range(0, 5)
                .mapToObj(i -> consumer.poll(Duration.ofSeconds(1)))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        Assertions.assertEquals(
            1, records.stream().filter(record -> record.key().equals(key)).count());

        // those data are in active segment, so they can be compacted.
        Assertions.assertEquals(
            10, records.stream().filter(record -> record.key().equals(anotherKey)).count());
      }
    }
  }

  @Test
  void testBrokers() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(Utils.randomString()).numberOfPartitions(6).create();
      Utils.sleep(Duration.ofSeconds(2));
      var brokers = admin.brokers();
      Assertions.assertEquals(3, brokers.size());
      brokers.forEach(broker -> Assertions.assertNotEquals(0, broker.config().raw().size()));
      Assertions.assertEquals(1, brokers.stream().filter(Broker::isController).count());
      brokers.forEach(
          broker -> Assertions.assertNotEquals(0, broker.topicPartitionLeaders().size()));
    }
  }

  @Test
  void testBrokerFolders() {
    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertEquals(brokerIds().size(), admin.brokers().size());
      // list all
      logFolders()
          .forEach(
              (id, ds) -> Assertions.assertEquals(admin.brokerFolders().get(id).size(), ds.size()));
    }
  }

  @Test
  void testReplicas() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic("abc").numberOfPartitions(2).create();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          2,
          admin.replicas(Set.of("abc")).stream()
              .collect(
                  Collectors.groupingBy(
                      replica -> TopicPartition.of(replica.topic(), replica.partition())))
              .size());
      var count =
          admin.replicas(admin.topicNames()).stream()
              .collect(
                  Collectors.groupingBy(
                      replica -> TopicPartition.of(replica.topic(), replica.partition())))
              .size();
      Assertions.assertEquals(
          count,
          admin.replicas().stream()
              .collect(
                  Collectors.groupingBy(
                      replica -> TopicPartition.of(replica.topic(), replica.partition())))
              .size());
    }
  }

  @Test
  void testReplicasPreferredLeaderFlag() {
    // arrange
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = "testReplicasPreferredLeaderFlag_" + Utils.randomString();
      var partitionCount = 10;
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitionCount)
          .numberOfReplicas((short) 3)
          .create();
      Utils.sleep(Duration.ofSeconds(3));
      var expectedPreferredLeader =
          IntStream.range(0, partitionCount)
              .mapToObj(p -> TopicPartition.of(topic, p))
              .collect(Collectors.toUnmodifiableMap(p -> p, p -> List.of(0)));
      var currentPreferredLeader =
          (Supplier<Map<TopicPartition, List<Integer>>>)
              () ->
                  admin.replicas(Set.of(topic)).stream()
                      .filter(Replica::isPreferredLeader)
                      .collect(
                          Collectors.groupingBy(
                              replica -> TopicPartition.of(replica.topic(), replica.partition()),
                              Collectors.mapping(
                                  replica -> replica.nodeInfo().id(), Collectors.toList())));

      // act, make 0 be the preferred leader of every partition
      IntStream.range(0, partitionCount)
          .forEach(p -> admin.migrator().partition(topic, p).moveTo(List.of(0, 1, 2)));
      Utils.sleep(Duration.ofSeconds(3));

      // assert
      Assertions.assertEquals(expectedPreferredLeader, currentPreferredLeader.get());
    }
  }

  @Test
  void testProducerStates() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    try (var producer = Producer.of(bootstrapServers());
        var admin = Admin.of(bootstrapServers())) {
      producer.sender().topic(topic).value(new byte[1]).run().toCompletableFuture().get();

      var states = admin.producerStates();
      Assertions.assertNotEquals(0, states.size());
      var producerState =
          states.stream()
              .filter(s -> s.topic().equals(topic))
              .collect(Collectors.toUnmodifiableList());
      Assertions.assertEquals(1, producerState.size());
    }
  }

  @Test
  void testIpQuota() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.quotaCreator().ip("192.168.11.11").connectionRate(10).create();
      Utils.sleep(Duration.ofSeconds(2));

      java.util.function.Consumer<List<Quota>> checker =
          (quotas) -> {
            Assertions.assertEquals(1, quotas.size());
            Assertions.assertEquals(
                Set.of(Quota.Target.IP),
                quotas.stream().map(Quota::target).collect(Collectors.toSet()));
            Assertions.assertEquals(
                Set.of(Quota.Limit.IP_CONNECTION_RATE),
                quotas.stream().map(Quota::limit).collect(Collectors.toSet()));
            Assertions.assertEquals("192.168.11.11", quotas.iterator().next().targetValue());
            Assertions.assertEquals(10, quotas.iterator().next().limitValue());
          };

      // only target
      checker.accept(
          admin.quotas(Quota.Target.IP).stream().collect(Collectors.toUnmodifiableList()));

      // only target and name
      checker.accept(
          admin.quotas(Quota.Target.IP, "192.168.11.11").stream()
              .collect(Collectors.toUnmodifiableList()));
    }
  }

  @Test
  void testMultipleIpQuota() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.quotaCreator().ip("192.168.11.11").connectionRate(10).create();
      admin.quotaCreator().ip("192.168.11.11").connectionRate(12).create();
      admin.quotaCreator().ip("192.168.11.11").connectionRate(9).create();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(1, admin.quotas(Quota.Target.IP, "192.168.11.11").size());
    }
  }

  @Test
  void testClientQuota() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .quotaCreator()
          .clientId("my-id")
          .produceRate(DataRate.Byte.of(10L).perSecond())
          .consumeRate(DataRate.Byte.of(100L).perSecond())
          .create();
      Utils.sleep(Duration.ofSeconds(2));

      java.util.function.Consumer<List<Quota>> checker =
          (quotas) -> {
            Assertions.assertEquals(2, quotas.size());
            Assertions.assertEquals(
                Set.of(Quota.Target.CLIENT_ID),
                quotas.stream().map(Quota::target).collect(Collectors.toSet()));
            Assertions.assertEquals(
                Set.of(Quota.Limit.PRODUCER_BYTE_RATE, Quota.Limit.CONSUMER_BYTE_RATE),
                quotas.stream().map(Quota::limit).collect(Collectors.toSet()));
            Assertions.assertEquals(
                10,
                quotas.stream()
                    .filter(q -> q.limit() == Quota.Limit.PRODUCER_BYTE_RATE)
                    .findFirst()
                    .get()
                    .limitValue());
            Assertions.assertEquals(
                "my-id",
                quotas.stream()
                    .filter(q -> q.limit() == Quota.Limit.PRODUCER_BYTE_RATE)
                    .findFirst()
                    .get()
                    .targetValue());
            Assertions.assertEquals(
                100,
                quotas.stream()
                    .filter(q -> q.limit() == Quota.Limit.CONSUMER_BYTE_RATE)
                    .findFirst()
                    .get()
                    .limitValue());
            Assertions.assertEquals(
                "my-id",
                quotas.stream()
                    .filter(q -> q.limit() == Quota.Limit.CONSUMER_BYTE_RATE)
                    .findFirst()
                    .get()
                    .targetValue());
          };

      // only target
      checker.accept(
          admin.quotas(Quota.Target.CLIENT_ID).stream().collect(Collectors.toUnmodifiableList()));

      // only target and name
      checker.accept(
          admin.quotas(Quota.Target.CLIENT_ID, "my-id").stream()
              .collect(Collectors.toUnmodifiableList()));
    }
  }

  @Test
  void testMultipleClientQuota() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .quotaCreator()
          .clientId("my-id")
          .consumeRate(DataRate.Byte.of(100L).perSecond())
          .create();
      admin
          .quotaCreator()
          .clientId("my-id")
          .produceRate(DataRate.Byte.of(1000L).perSecond())
          .create();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(2, admin.quotas(Quota.Target.CLIENT_ID, "my-id").size());
    }
  }

  @Test
  void testClusterInfo() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      String topic0 = "testClusterInfoFromAdmin_" + Utils.randomString(8);
      String topic1 = "testClusterInfoFromAdmin_" + Utils.randomString(8);
      String topic2 = "testClusterInfoFromAdmin_" + Utils.randomString(8);
      int partitionCount = 10;
      short replicaCount = 2;

      Stream.of(topic0, topic1, topic2)
          .forEach(
              topicName ->
                  admin
                      .creator()
                      .topic(topicName)
                      .numberOfPartitions(partitionCount)
                      .numberOfReplicas(replicaCount)
                      .create());
      Utils.sleep(Duration.ofSeconds(2));

      final var clusterInfo = admin.clusterInfo(Set.of(topic0, topic1, topic2));

      // ClusterInfo#nodes
      Assertions.assertEquals(
          brokerIds(),
          clusterInfo.nodes().stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet()));
      // ClusterInfo#topics
      Assertions.assertEquals(Set.of(topic0, topic1, topic2), clusterInfo.topics());
      // ClusterInfo#replicas
      Assertions.assertEquals(partitionCount * replicaCount, clusterInfo.replicas(topic0).size());
      Assertions.assertEquals(partitionCount * replicaCount, clusterInfo.replicas(topic1).size());
      Assertions.assertEquals(partitionCount * replicaCount, clusterInfo.replicas(topic2).size());
      // ClusterInfo#availableReplicas
      Assertions.assertEquals(
          partitionCount * replicaCount, clusterInfo.availableReplicas(topic0).size());
      Assertions.assertEquals(
          partitionCount * replicaCount, clusterInfo.availableReplicas(topic1).size());
      Assertions.assertEquals(
          partitionCount * replicaCount, clusterInfo.availableReplicas(topic2).size());
      // ClusterInfo#availableReplicaLeaders
      Assertions.assertEquals(partitionCount, clusterInfo.replicaLeaders(topic0).size());
      Assertions.assertEquals(partitionCount, clusterInfo.replicaLeaders(topic1).size());
      Assertions.assertEquals(partitionCount, clusterInfo.replicaLeaders(topic2).size());
      // No resource match found will raise exception
      Assertions.assertEquals(List.of(), clusterInfo.replicas("Unknown Topic"));
      Assertions.assertEquals(List.of(), clusterInfo.availableReplicas("Unknown Topic"));
      Assertions.assertEquals(List.of(), clusterInfo.replicaLeaders("Unknown Topic"));
      Assertions.assertThrows(NoSuchElementException.class, () -> clusterInfo.node(-1));

      admin
          .topicPartitions(Set.of(topic0, topic1, topic2))
          .forEach(
              p ->
                  Assertions.assertNotEquals(
                      Optional.empty(), clusterInfo.replicaLeader(p), "partition: " + p));
      Set.of(topic0, topic1, topic2)
          .forEach(
              t ->
                  brokerIds()
                      .forEach(
                          id -> Assertions.assertNotEquals(0, clusterInfo.replicas(id, t).size())));
      admin
          .topicPartitions(Set.of(topic0, topic1, topic2))
          .forEach(p -> Assertions.assertNotEquals(0, clusterInfo.replicas(p).size()));
    }
  }

  @ParameterizedTest
  @ValueSource(shorts = {1, 2, 3})
  void preferredLeaderElection(short replicaSize) {
    var clusterSize = brokerIds().size();
    var topic = "preferredLeaderElection_" + Utils.randomString(6);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(30).numberOfReplicas(replicaSize).create();
      Utils.sleep(Duration.ofSeconds(3));

      var topicPartitions =
          IntStream.range(0, 30)
              .mapToObj(i -> TopicPartition.of(topic, i))
              .collect(Collectors.toUnmodifiableSet());

      var currentLeaderMap =
          (Supplier<Map<TopicPartition, Integer>>)
              () ->
                  admin.replicas(Set.of(topic)).stream()
                      .filter(Replica::isLeader)
                      .collect(
                          Utils.toSortedMap(
                              replica -> TopicPartition.of(replica.topic(), replica.partition()),
                              replica -> replica.nodeInfo().id()));
      var expectedReplicaList =
          currentLeaderMap.get().entrySet().stream()
              .collect(
                  Utils.toSortedMap(
                      Map.Entry::getKey,
                      entry -> {
                        int leaderBroker = entry.getValue();
                        return List.of(
                                (leaderBroker + 2) % clusterSize,
                                leaderBroker, // original leader
                                (leaderBroker + 1) % clusterSize)
                            .subList(0, replicaSize);
                      }));
      var expectedLeaderMap =
          (Supplier<Map<TopicPartition, Integer>>)
              () ->
                  expectedReplicaList.entrySet().stream()
                      .collect(
                          Utils.toSortedMap(
                              Map.Entry::getKey,
                              e -> e.getValue().stream().findFirst().orElseThrow()));

      // change replica list
      topicPartitions.forEach(
          topicPartition ->
              admin
                  .migrator()
                  .partition(topicPartition.topic(), topicPartition.partition())
                  .moveTo(expectedReplicaList.get(topicPartition)));
      Utils.sleep(Duration.ofSeconds(8));

      // ReplicaMigrator#moveTo will trigger leader election if current leader being kicked out of
      // replica list. This case is always true for replica size equals to 1.
      if (replicaSize == 1) {
        // ReplicaMigrator#moveTo will trigger leader election implicitly if the original leader is
        // kicked out of the replica list. Test if ReplicaMigrator#moveTo actually trigger leader
        // election implicitly.
        Assertions.assertEquals(expectedLeaderMap.get(), currentLeaderMap.get());

        // act, the Admin#preferredLeaderElection won't throw a ElectionNotNeededException
        topicPartitions.forEach(admin::preferredLeaderElection);
        Utils.sleep(Duration.ofSeconds(2));

        // after election
        Assertions.assertEquals(expectedLeaderMap.get(), currentLeaderMap.get());
      } else {
        // before election
        Assertions.assertNotEquals(expectedLeaderMap.get(), currentLeaderMap.get());

        // act
        topicPartitions.forEach(admin::preferredLeaderElection);
        Utils.sleep(Duration.ofSeconds(2));

        // after election
        Assertions.assertEquals(expectedLeaderMap.get(), currentLeaderMap.get());
      }
    }
  }

  @Test
  void testTransactionIds() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      Assertions.assertTrue(producer.transactional());
      producer.sender().key(new byte[10]).topic(topicName).run().toCompletableFuture().get();

      Assertions.assertTrue(admin.transactionIds().contains(producer.transactionId().get()));

      var transaction =
          admin.transactions(admin.transactionIds()).stream()
              .filter(t -> t.transactionId().equals(producer.transactionId().get()))
              .findFirst()
              .get();
      Assertions.assertNotNull(transaction);
      Assertions.assertEquals(
          transaction.state() == TransactionState.COMPLETE_COMMIT ? 0 : 1,
          transaction.topicPartitions().size());
    }
  }

  @Test
  void testTransactionIdsWithMultiPuts() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer =
            Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional()) {
      Assertions.assertTrue(producer.transactional());
      IntStream.range(0, 10)
          .forEach(
              index ->
                  producer
                      .sender()
                      .key(String.valueOf(index).getBytes(UTF_8))
                      .topic(topicName)
                      .run());
      producer.flush();

      Assertions.assertTrue(admin.transactionIds().contains(producer.transactionId().get()));
      Utils.waitFor(
          () ->
              admin.transactions(admin.transactionIds()).stream()
                      .filter(t -> t.transactionId().equals(producer.transactionId().get()))
                      .findFirst()
                      .get()
                      .state()
                  == TransactionState.COMPLETE_COMMIT);
      Utils.waitFor(
          () ->
              admin.transactions(admin.transactionIds()).stream()
                  .filter(t -> t.transactionId().equals(producer.transactionId().get()))
                  .findFirst()
                  .get()
                  .topicPartitions()
                  .isEmpty());
    }
  }

  @Test
  void testRemoveAllMembers() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.builder().bootstrapServers(bootstrapServers()).build();
        var consumer =
            Consumer.forTopics(Set.of(topicName))
                .bootstrapServers(bootstrapServers())
                .fromBeginning()
                .build()) {
      producer.sender().topic(topicName).key(new byte[10]).run();
      producer.flush();
      Assertions.assertEquals(1, consumer.poll(1, Duration.ofSeconds(5)).size());

      producer.sender().topic(topicName).key(new byte[10]).run();
      producer.flush();
      admin.removeAllMembers(consumer.groupId());
      Assertions.assertEquals(
          0,
          admin.consumerGroups(Set.of(consumer.groupId())).stream()
              .filter(g -> g.groupId().equals(consumer.groupId()))
              .findFirst()
              .get()
              .assignment()
              .size());
    }
  }

  @Test
  void testRemoveEmptyMember() {
    var topicName = Utils.randomString(10);
    var groupId = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      try (var consumer =
          Consumer.forTopics(Set.of(topicName))
              .bootstrapServers(bootstrapServers())
              .groupId(groupId)
              .groupInstanceId(Utils.randomString(10))
              .build()) {
        Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
      }
      Assertions.assertEquals(
          1,
          admin.consumerGroups(Set.of(groupId)).stream()
              .filter(g -> g.groupId().equals(groupId))
              .findFirst()
              .get()
              .assignment()
              .size());
      admin.removeAllMembers(groupId);
      Assertions.assertEquals(
          0,
          admin.consumerGroups(Set.of(groupId)).stream()
              .filter(g -> g.groupId().equals(groupId))
              .findFirst()
              .get()
              .assignment()
              .size());
      admin.removeAllMembers(groupId);
    }
  }

  @Test
  void testRemoveStaticMembers() {
    var topicName = Utils.randomString(10);
    var staticId = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.builder().bootstrapServers(bootstrapServers()).build();
        var consumer =
            Consumer.forTopics(Set.of(topicName))
                .bootstrapServers(bootstrapServers())
                .groupInstanceId(staticId)
                .fromBeginning()
                .build()) {
      producer.sender().topic(topicName).key(new byte[10]).run();
      producer.flush();
      Assertions.assertEquals(1, consumer.poll(1, Duration.ofSeconds(5)).size());

      producer.sender().topic(topicName).key(new byte[10]).run();
      producer.flush();
      admin.removeStaticMembers(consumer.groupId(), Set.of(consumer.groupInstanceId().get()));
      Assertions.assertEquals(
          0,
          admin.consumerGroups(Set.of(consumer.groupId())).stream()
              .filter(g -> g.groupId().equals(consumer.groupId()))
              .findFirst()
              .get()
              .assignment()
              .size());
    }
  }

  @Test
  void testRemoveGroupWithDynamicMembers() {
    var groupId = Utils.randomString(10);
    var topicName = Utils.randomString(10);
    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .groupId(groupId)
            .build()) {
      Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
    }
    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertTrue(admin.consumerGroupIds().contains(groupId));
      admin.removeGroup(groupId);
      Assertions.assertFalse(admin.consumerGroupIds().contains(groupId));
    }
  }

  @Test
  void testRemoveGroupWithStaticMembers() {
    var groupId = Utils.randomString(10);
    var topicName = Utils.randomString(10);
    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .groupId(groupId)
            .groupInstanceId(Utils.randomString(10))
            .build()) {
      Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
    }

    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertTrue(admin.consumerGroupIds().contains(groupId));
      // the static member is existent
      var astraeaExecutionRuntimeException =
          Assertions.assertThrows(
              ExecutionRuntimeException.class, () -> admin.removeGroup(groupId));
      Assertions.assertEquals(
          GroupNotEmptyException.class, astraeaExecutionRuntimeException.getRootCause().getClass());
      // cleanup members
      admin.removeAllMembers(groupId);
      admin.removeGroup(groupId);
      Assertions.assertFalse(admin.consumerGroupIds().contains(groupId));
    }
  }

  @RepeatedTest(5) // this test is timer-based, so it needs to be looped for stability
  void testReassignmentWhenMovingPartitionToAnotherBroker() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(3));

      var currentBroker =
          admin.replicas(Set.of(topicName)).stream()
              .filter(replica -> replica.partition() == 0)
              .findFirst()
              .get()
              .nodeInfo()
              .id();
      var nextBroker = brokerIds().stream().filter(i -> i != currentBroker).findAny().get();

      try (var producer = Producer.of(bootstrapServers())) {
        var done = new AtomicBoolean(false);
        var data = new byte[1000];
        var f =
            CompletableFuture.runAsync(
                () -> {
                  while (!done.get())
                    producer.sender().topic(topicName).key(data).value(data).run();
                });
        try {
          admin.migrator().topic(topicName).moveTo(List.of(nextBroker));
          var addingReplicas = admin.addingReplicas(Set.of(topicName));

          // Don't verify the result if the migration is done
          if (!addingReplicas.isEmpty()) {
            Assertions.assertEquals(1, addingReplicas.size());
            Assertions.assertEquals(nextBroker, addingReplicas.get(0).broker());
          }
        } finally {
          done.set(true);
          Utils.swallowException(f::get);
        }
      }
    }
  }

  @RepeatedTest(5) // this test is timer-based, so it needs to be looped for stability
  void testReassignmentWhenMovingPartitionToAnotherPath() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(3));

      var currentReplica =
          admin.replicas(Set.of(topicName)).stream()
              .filter(replica -> replica.partition() == 0)
              .findFirst()
              .get();
      var currentBroker = currentReplica.nodeInfo().id();
      var currentPath = currentReplica.dataFolder();
      var nextPath =
          logFolders().get(currentBroker).stream()
              .filter(p -> !p.equals(currentPath))
              .findFirst()
              .get();

      try (var producer = Producer.of(bootstrapServers())) {
        var done = new AtomicBoolean(false);
        var data = new byte[1000];
        var f =
            CompletableFuture.runAsync(
                () -> {
                  while (!done.get())
                    producer.sender().topic(topicName).key(data).value(data).run();
                });

        try {
          admin
              .migrator()
              .topic(topicName)
              .moveTo(Map.of(currentReplica.nodeInfo().id(), nextPath));
          var addingReplicas = admin.addingReplicas(Set.of(topicName));
          // Don't verify the result if the migration is done
          if (!addingReplicas.isEmpty()) {
            Assertions.assertEquals(1, addingReplicas.size());
            Assertions.assertEquals(currentBroker, addingReplicas.get(0).broker());
            Assertions.assertEquals(nextPath, addingReplicas.get(0).path());
          }
        } finally {
          done.set(true);
          Utils.swallowException(f::get);
        }
      }
    }
  }

  @RepeatedTest(5) // this test is timer-based, so it needs to be looped for stability
  void testMultiReassignments() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(1).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(3));

      var brokers = new ArrayList<>(brokerIds()).subList(0, 2);
      try (var producer = Producer.of(bootstrapServers())) {
        var done = new AtomicBoolean(false);
        var data = new byte[1000];
        var f =
            CompletableFuture.runAsync(
                () -> {
                  while (!done.get())
                    producer.sender().topic(topicName).key(data).value(data).run();
                });
        try {
          admin.migrator().topic(topicName).moveTo(brokers);
          var addingReplicas = admin.addingReplicas(Set.of(topicName));
          // Don't verify the result if the migration is done
          if (!addingReplicas.isEmpty()) Assertions.assertEquals(2, addingReplicas.size());

        } finally {
          done.set(true);
          Utils.swallowException(f::get);
        }
      }
    }
  }

  @Test
  void testReassignmentWithNothing() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(1).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(3));
      try (var producer = Producer.of(bootstrapServers())) {
        producer.sender().topic(topicName).value(new byte[100]).run();
        producer.flush();
      }
      Assertions.assertEquals(0, admin.addingReplicas(Set.of(topicName)).size());
    }
  }

  @Test
  void testDeleteRecord() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(2));
      var deleteRecords = admin.deleteRecords(Map.of(TopicPartition.of(topicName, 0), 0L));

      Assertions.assertEquals(1, deleteRecords.size());
      Assertions.assertEquals(0, deleteRecords.values().stream().findFirst().get().lowWatermark());

      try (var producer = Producer.of(bootstrapServers())) {
        var senders =
            Stream.of(0, 0, 0, 1, 1)
                .map(x -> producer.sender().topic(topicName).partition(x).value(new byte[100]))
                .collect(Collectors.toList());
        producer.send(senders);
        producer.flush();
      }

      deleteRecords =
          admin.deleteRecords(
              Map.of(TopicPartition.of(topicName, 0), 2L, TopicPartition.of(topicName, 1), 1L));
      Assertions.assertEquals(2, deleteRecords.size());
      Assertions.assertEquals(2, deleteRecords.get(TopicPartition.of(topicName, 0)).lowWatermark());
      Assertions.assertEquals(1, deleteRecords.get(TopicPartition.of(topicName, 1)).lowWatermark());

      var partitions = admin.partitions(Set.of(topicName));
      Assertions.assertEquals(3, partitions.size());
      Assertions.assertEquals(
          2,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 0)
              .findFirst()
              .get()
              .earliestOffset());
      Assertions.assertEquals(
          1,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 1)
              .findFirst()
              .get()
              .earliestOffset());
      Assertions.assertEquals(
          0,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 2)
              .findFirst()
              .get()
              .earliestOffset());
    }
  }

  @Test
  void testDeleteTopic() {
    var topicNames =
        IntStream.range(0, 4).mapToObj(x -> Utils.randomString(10)).collect(Collectors.toList());

    try (var admin = Admin.of(bootstrapServers())) {
      topicNames.forEach(
          x -> admin.creator().topic(x).numberOfPartitions(3).numberOfReplicas((short) 3).create());
      Utils.sleep(Duration.ofSeconds(2));

      admin.deleteTopics(Set.of(topicNames.get(0), topicNames.get(1)));
      Utils.sleep(Duration.ofSeconds(2));

      var latestTopicNames = admin.topicNames();
      Assertions.assertFalse(latestTopicNames.contains(topicNames.get(0)));
      Assertions.assertFalse(latestTopicNames.contains(topicNames.get(1)));
      Assertions.assertTrue(latestTopicNames.contains(topicNames.get(2)));
      Assertions.assertTrue(latestTopicNames.contains(topicNames.get(3)));

      admin.deleteTopics(Set.of(topicNames.get(3)));
      Utils.sleep(Duration.ofSeconds(2));

      latestTopicNames = admin.topicNames();
      Assertions.assertFalse(latestTopicNames.contains(topicNames.get(3)));
      Assertions.assertTrue(latestTopicNames.contains(topicNames.get(2)));
    }
  }

  @Test
  void testReplicationThrottler$egress$ingress$dataRate() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin
          .replicationThrottler()
          .egress(DataRate.MiB.of(72).perSecond())
          .ingress(DataRate.MiB.of(32).perSecond())
          .apply();
      Utils.sleep(Duration.ofMillis(100));
      brokerIds()
          .forEach(
              id -> {
                var leaderValue =
                    admin.brokers().stream()
                        .filter(n -> n.id() == id)
                        .findFirst()
                        .get()
                        .config()
                        .value("leader.replication.throttled.rate")
                        .orElseThrow();
                var followerValue =
                    admin.brokers().stream()
                        .filter(n -> n.id() == id)
                        .findFirst()
                        .get()
                        .config()
                        .value("follower.replication.throttled.rate")
                        .orElseThrow();

                Assertions.assertEquals(
                    DataSize.MiB.of(72), DataSize.Byte.of(Long.parseLong(leaderValue)));
                Assertions.assertEquals(
                    DataSize.MiB.of(32), DataSize.Byte.of(Long.parseLong(followerValue)));
              });
    }
  }

  @Test
  void testReplicationThrottler$egress$ingress$map() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var rate0 = DataRate.MiB.of(29).perSecond();
      var rate1 = DataRate.MiB.of(61).perSecond();
      var rate2 = DataRate.MiB.of(97).perSecond();
      admin
          .replicationThrottler()
          .egress(rate0)
          .ingress(rate0)
          .egress(Map.of(1, rate1, 2, rate2))
          .ingress(Map.of(1, rate1, 2, rate2))
          .apply();
      Utils.sleep(Duration.ofMillis(100));
      Map.of(0, rate0, 1, rate1, 2, rate2)
          .forEach(
              (id, rate) -> {
                var leaderValue =
                    admin.brokers().stream()
                        .filter(n -> n.id() == id)
                        .findFirst()
                        .get()
                        .config()
                        .value("leader.replication.throttled.rate")
                        .orElseThrow();
                var followerValue =
                    admin.brokers().stream()
                        .filter(n -> n.id() == id)
                        .findFirst()
                        .get()
                        .config()
                        .value("follower.replication.throttled.rate")
                        .orElseThrow();

                Assertions.assertEquals(
                    rate.dataSize(), DataSize.Byte.of(Long.parseLong(leaderValue)));
                Assertions.assertEquals(
                    rate.dataSize(), DataSize.Byte.of(Long.parseLong(followerValue)));
              });
    }
  }

  private Set<TopicPartitionReplica> currentLeaderLogs(Admin admin, String topic) {
    return admin.replicas(Set.of(topic)).stream()
        .filter(Replica::isLeader)
        .map(
            replica ->
                TopicPartitionReplica.of(
                    replica.topic(), replica.partition(), replica.nodeInfo().id()))
        .collect(Collectors.toUnmodifiableSet());
  }

  private Set<TopicPartitionReplica> currentFollowerLogs(Admin admin, String topic) {
    return admin.replicas(Set.of(topic)).stream()
        .filter(replica -> !replica.isLeader())
        .map(
            replica ->
                TopicPartitionReplica.of(
                    replica.topic(), replica.partition(), replica.nodeInfo().id()))
        .collect(Collectors.toUnmodifiableSet());
  }

  @Test
  void testReplicationThrottler$throttle$topic() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      var partitions = ThreadLocalRandom.current().nextInt(5, 20);
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitions)
          .numberOfReplicas((short) 3)
          .create();
      Utils.sleep(Duration.ofSeconds(1));
      admin.replicationThrottler().throttle(topic).apply();
      Utils.sleep(Duration.ofSeconds(1));

      Assertions.assertEquals(currentLeaderLogs(admin, topic), fetchLeaderThrottle(topic));
      Assertions.assertEquals(currentFollowerLogs(admin, topic), fetchFollowerThrottle(topic));
    }
  }

  @Test
  void testReplicationThrottler$throttle$partition() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      var partitions = ThreadLocalRandom.current().nextInt(5, 20);
      var selectedPartition = ThreadLocalRandom.current().nextInt(0, partitions);
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitions)
          .numberOfReplicas((short) 3)
          .create();
      Utils.sleep(Duration.ofSeconds(1));
      admin.replicationThrottler().throttle(TopicPartition.of(topic, selectedPartition)).apply();
      Utils.sleep(Duration.ofSeconds(1));

      var expectedLeaderLogs =
          currentLeaderLogs(admin, topic).stream()
              .filter(x -> x.partition() == selectedPartition)
              .collect(Collectors.toUnmodifiableSet());
      var expectedFollowerLogs =
          currentFollowerLogs(admin, topic).stream()
              .filter(x -> x.partition() == selectedPartition)
              .collect(Collectors.toUnmodifiableSet());

      Assertions.assertEquals(expectedLeaderLogs, fetchLeaderThrottle(topic));
      Assertions.assertEquals(expectedFollowerLogs, fetchFollowerThrottle(topic));
    }
  }

  @Test
  void testReplicationThrottler$throttle$replica() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      var partitions = 20;
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitions)
          .numberOfReplicas((short) 3)
          .create();
      Utils.sleep(Duration.ofSeconds(1));

      var selectedLeader =
          currentLeaderLogs(admin, topic).stream()
              .sorted(Comparator.comparing(TopicPartitionReplica::hashCode))
              .limit(10)
              .collect(Collectors.toUnmodifiableSet());
      var selectedFollower =
          currentFollowerLogs(admin, topic).stream()
              .sorted(Comparator.comparing(TopicPartitionReplica::hashCode))
              .limit(25)
              .collect(Collectors.toUnmodifiableSet());
      var allTargetLog =
          Stream.concat(selectedLeader.stream(), selectedFollower.stream())
              .collect(Collectors.toUnmodifiableSet());

      var replicationThrottler = admin.replicationThrottler();
      allTargetLog.forEach(replicationThrottler::throttle);
      replicationThrottler.apply();
      Utils.sleep(Duration.ofSeconds(1));

      Assertions.assertEquals(allTargetLog, fetchLeaderThrottle(topic));
      Assertions.assertEquals(allTargetLog, fetchFollowerThrottle(topic));
    }
  }

  @Test
  void testReplicationThrottler$throttle$Leader$Follower() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      var partitions = 20;
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitions)
          .numberOfReplicas((short) 3)
          .create();
      Utils.sleep(Duration.ofSeconds(1));

      var selectedLeader =
          IntStream.range(0, 10)
              .mapToObj(
                  i ->
                      TopicPartitionReplica.of(
                          topic,
                          ThreadLocalRandom.current().nextInt(0, 20),
                          ThreadLocalRandom.current().nextInt(0, 3)))
              .collect(Collectors.toUnmodifiableSet());
      var selectedFollower =
          IntStream.range(0, 25)
              .mapToObj(
                  i ->
                      TopicPartitionReplica.of(
                          topic,
                          ThreadLocalRandom.current().nextInt(0, 20),
                          ThreadLocalRandom.current().nextInt(0, 3)))
              .collect(Collectors.toUnmodifiableSet());

      var replicationThrottler = admin.replicationThrottler();
      selectedLeader.forEach(replicationThrottler::throttleLeader);
      selectedFollower.forEach(replicationThrottler::throttleFollower);
      replicationThrottler.apply();
      Utils.sleep(Duration.ofSeconds(1));

      Assertions.assertEquals(selectedLeader, fetchLeaderThrottle(topic));
      Assertions.assertEquals(selectedFollower, fetchFollowerThrottle(topic));
    }
  }

  @Test
  @DisplayName("Append works")
  void testReplicationThrottler$throttle$append$config() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      admin.creator().topic(topic).numberOfPartitions(3).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));

      var log0 = TopicPartitionReplica.of(topic, 0, 0);
      var log1 = TopicPartitionReplica.of(topic, 1, 1);
      var log2 = TopicPartitionReplica.of(topic, 2, 2);

      admin.replicationThrottler().throttle(log0).apply();
      admin.replicationThrottler().throttle(log1).apply();
      admin.replicationThrottler().throttle(log2).apply();
      Assertions.assertEquals(Set.of(log0, log1, log2), fetchLeaderThrottle(topic));
      Assertions.assertEquals(Set.of(log0, log1, log2), fetchFollowerThrottle(topic));
    }
  }

  @Test
  @DisplayName("The API can't be use in conjunction with wildcard throttle")
  void testReplicationThrottler$throttle$wildcard() {
    try (AdminClient adminClient =
        AdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()))) {
      try (Admin admin = Admin.of(bootstrapServers())) {
        var topic = Utils.randomString();
        admin.creator().topic(topic).numberOfPartitions(10).numberOfReplicas((short) 3).create();
        Utils.sleep(Duration.ofSeconds(1));

        var configEntry0 = new ConfigEntry("leader.replication.throttled.replicas", "*");
        var configEntry1 = new ConfigEntry("follower.replication.throttled.replicas", "*");
        var alter0 = new AlterConfigOp(configEntry0, AlterConfigOp.OpType.SET);
        var alter1 = new AlterConfigOp(configEntry1, AlterConfigOp.OpType.SET);
        var configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        adminClient.incrementalAlterConfigs(Map.of(configResource, List.of(alter0, alter1)));
        Utils.sleep(Duration.ofSeconds(1));

        var topicPartition = TopicPartition.of(topic, 0);
        var topicPartitionReplica = TopicPartitionReplica.of(topic, 0, 0);
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> admin.replicationThrottler().throttle(topic).apply());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> admin.replicationThrottler().throttle(topicPartition).apply());
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> admin.replicationThrottler().throttle(topicPartitionReplica).apply());
      }
    }
  }

  private Set<TopicPartitionReplica> fetchLogThrottleConfig(String configKey, String topicName) {
    try (var admin = Admin.of(bootstrapServers())) {
      return admin
          .topics(Set.of(topicName))
          .get(0)
          .config()
          .value(configKey)
          .filter(v -> !v.isEmpty())
          .map(
              value ->
                  Arrays.stream(value.split(","))
                      .map(x -> x.split(":"))
                      .map(
                          x ->
                              TopicPartitionReplica.of(
                                  topicName, Integer.parseInt(x[0]), Integer.parseInt(x[1])))
                      .collect(Collectors.toUnmodifiableSet()))
          .orElse(Set.of());
    }
  }

  private Set<TopicPartitionReplica> fetchLeaderThrottle(String topicName) {
    return fetchLogThrottleConfig("leader.replication.throttled.replicas", topicName);
  }

  private Set<TopicPartitionReplica> fetchFollowerThrottle(String topicName) {
    return fetchLogThrottleConfig("follower.replication.throttled.replicas", topicName);
  }

  @Test
  void testClearTopicReplicationThrottle() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      admin.creator().topic(topic).numberOfPartitions(10).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));
      admin.replicationThrottler().throttle(topic).apply();
      Utils.sleep(Duration.ofSeconds(1));

      admin.clearReplicationThrottle(topic);
      Utils.sleep(Duration.ofSeconds(1));

      Assertions.assertEquals(Set.of(), fetchLeaderThrottle(topic));
      Assertions.assertEquals(Set.of(), fetchFollowerThrottle(topic));
    }
  }

  @Test
  void testClearPartitionReplicationThrottle() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      admin.creator().topic(topic).numberOfPartitions(10).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));
      admin.replicationThrottler().throttle(topic).apply();
      Utils.sleep(Duration.ofSeconds(1));

      admin.clearReplicationThrottle(TopicPartition.of(topic, 0));
      Utils.sleep(Duration.ofSeconds(1));

      Assertions.assertTrue(
          fetchLeaderThrottle(topic).stream().allMatch(log -> log.partition() != 0));
      Assertions.assertTrue(
          fetchFollowerThrottle(topic).stream().allMatch(log -> log.partition() != 0));
    }
  }

  @Test
  void testClearReplicaReplicationThrottle() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      admin.creator().topic(topic).numberOfPartitions(10).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));
      admin.replicationThrottler().throttle(topic).apply();
      Utils.sleep(Duration.ofSeconds(1));

      admin.clearReplicationThrottle(TopicPartitionReplica.of(topic, 0, 0));
      Utils.sleep(Duration.ofSeconds(1));

      Assertions.assertTrue(
          fetchLeaderThrottle(topic).stream()
              .noneMatch(log -> log.partition() == 0 && log.brokerId() == 0));
      Assertions.assertTrue(
          fetchFollowerThrottle(topic).stream()
              .noneMatch(log -> log.partition() == 0 && log.brokerId() == 0));
    }
  }

  @Test
  void testClearIngressReplicationThrottle() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      // apply throttle
      var dataRate = DataRate.MiB.of(500).perSecond();
      admin.replicationThrottler().ingress(dataRate).apply();
      Utils.sleep(Duration.ofSeconds(1));

      // ensure the throttle was applied
      final var value0 =
          admin.brokers().stream()
              .filter(n -> n.id() == 0)
              .findFirst()
              .get()
              .config()
              .value("follower.replication.throttled.rate");
      Assertions.assertEquals((long) dataRate.byteRate(), Long.valueOf(value0.orElseThrow()));

      // clear throttle
      admin.clearIngressReplicationThrottle(Set.of(0, 1, 2));
      Utils.sleep(Duration.ofSeconds(1));

      // ensure the throttle was removed
      final var value1 =
          admin.brokers().stream()
              .filter(n -> n.id() == 0)
              .findFirst()
              .get()
              .config()
              .value("follower.replication.throttled.rate");
      Assertions.assertTrue(value1.isEmpty());
    }
  }

  @Test
  void testClearEgressReplicationThrottle() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      // apply throttle
      var dataRate = DataRate.MiB.of(500).perSecond();
      admin.replicationThrottler().egress(dataRate).apply();
      Utils.sleep(Duration.ofSeconds(1));

      // ensure the throttle was applied
      final var value0 =
          admin.brokers().stream()
              .filter(n -> n.id() == 0)
              .findFirst()
              .get()
              .config()
              .value("leader.replication.throttled.rate");
      Assertions.assertEquals((long) dataRate.byteRate(), Long.valueOf(value0.orElseThrow()));

      // clear throttle
      admin.clearEgressReplicationThrottle(Set.of(0, 1, 2));
      Utils.sleep(Duration.ofSeconds(1));

      // ensure the throttle was removed
      final var value1 =
          admin.brokers().stream()
              .filter(n -> n.id() == 0)
              .findFirst()
              .get()
              .config()
              .value("leader.replication.throttled.rate");
      Assertions.assertTrue(value1.isEmpty());
    }
  }

  @Test
  void testTopicNames() {
    var bootstrapServers = bootstrapServers();
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers);
        var producer = Producer.of(bootstrapServers);
        var consumer =
            Consumer.forTopics(Set.of(topicName)).bootstrapServers(bootstrapServers).build()) {
      // producer and consumer here are used to trigger kafka to create internal topic
      // __consumer_offsets
      producer
          .sender()
          .topic(topicName)
          .key("foo".getBytes(UTF_8))
          .run()
          .toCompletableFuture()
          .join();
      consumer.poll(Duration.ofSeconds(1));

      Assertions.assertTrue(
          admin.topicNames(true).stream().anyMatch(t -> t.equals("__consumer_offsets")));
      Assertions.assertEquals(
          1, admin.topicNames(true).stream().filter(t -> t.equals(topicName)).count());

      Assertions.assertFalse(
          admin.topicNames(false).stream().anyMatch(t -> t.equals("__consumer_offsets")));
      Assertions.assertEquals(
          1, admin.topicNames(false).stream().filter(t -> t.equals(topicName)).count());
    }
  }

  @Test
  void testClearLeaderReplicationThrottle() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      var log = TopicPartitionReplica.of(topic, 0, 0);
      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));
      admin.replicationThrottler().throttleLeader(log).apply();
      Utils.sleep(Duration.ofSeconds(1));
      Assertions.assertEquals(
          "0:0",
          admin
              .topics(Set.of(topic))
              .get(0)
              .config()
              .value("leader.replication.throttled.replicas")
              .orElse(""));

      admin.clearLeaderReplicationThrottle(log);
      Utils.sleep(Duration.ofSeconds(1));
      Assertions.assertEquals(
          "",
          admin
              .topics(Set.of(topic))
              .get(0)
              .config()
              .value("leader.replication.throttled.replicas")
              .orElse(""));
    }
  }

  @Test
  void testClearFollowerReplicationThrottle() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      var log = TopicPartitionReplica.of(topic, 0, 0);
      admin.creator().topic(topic).numberOfPartitions(1).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));
      admin.replicationThrottler().throttleFollower(log).apply();
      Utils.sleep(Duration.ofSeconds(1));
      Assertions.assertEquals(
          "0:0",
          admin
              .topics(Set.of(topic))
              .get(0)
              .config()
              .value("follower.replication.throttled.replicas")
              .orElse(""));

      admin.clearFollowerReplicationThrottle(log);
      Utils.sleep(Duration.ofSeconds(1));
      Assertions.assertEquals(
          "",
          admin
              .topics(Set.of(topic))
              .get(0)
              .config()
              .value("follower.replication.throttled.replicas")
              .orElse(""));
    }
  }

  @Test
  void testReplicationThrottleAffectedTargets() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topic = Utils.randomString();
      var replica0 = TopicPartitionReplica.of(topic, 0, 0);
      var replica1 = TopicPartitionReplica.of(topic, 1, 1);
      var replica2 = TopicPartitionReplica.of(topic, 2, 2);
      admin.creator().topic(topic).numberOfPartitions(3).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));

      var affectedResources =
          admin
              .replicationThrottler()
              .ingress(Map.of(0, DataRate.GiB.of(1).perSecond()))
              .egress(Map.of(1, DataRate.GiB.of(1).perSecond()))
              .throttle(replica0)
              .throttleLeader(replica1)
              .throttleFollower(replica2)
              .apply();

      Assertions.assertEquals(Set.of(replica0, replica1), affectedResources.leaders());
      Assertions.assertEquals(Set.of(replica0, replica2), affectedResources.followers());
      Assertions.assertEquals(
          Map.of(0, DataRate.GiB.of(1).perSecond()).toString(),
          affectedResources.ingress().toString());
      Assertions.assertEquals(
          Map.of(1, DataRate.GiB.of(1).perSecond()).toString(),
          affectedResources.egress().toString());
    }
  }

  @Test
  void testMoveReplicaToAnotherFolder() {
    var topic = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).create();
      Utils.sleep(Duration.ofSeconds(2));

      var closed = new AtomicBoolean(false);
      var producerFuture =
          CompletableFuture.runAsync(
              () -> {
                var key = new byte[1024];
                try (var producer = Producer.of(bootstrapServers())) {
                  while (!closed.get()) {
                    producer.sender().key(key).topic(topic).run();
                  }
                }
              });
      Utils.sleep(Duration.ofSeconds(2));
      var replica =
          admin.replicas(Set.of(topic)).stream().filter(r -> r.partition() == 0).findFirst().get();
      admin
          .migrator()
          .partition(topic, 0)
          .moveTo(
              Map.of(
                  replica.nodeInfo().id(),
                  logFolders().get(replica.nodeInfo().id()).stream()
                      .filter(d -> !d.equals(replica.dataFolder()))
                      .findFirst()
                      .get()));
      Utils.waitFor(
          () ->
              admin.replicas(Set.of(topic)).stream().filter(r -> r.partition() == 0).count() == 2);
    }
  }

  @Test
  void testListPartitions() {
    var topic = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(2).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(2));
      var partitions = admin.partitions(Set.of(topic));
      Assertions.assertEquals(2, partitions.size());
      partitions.forEach(
          p -> {
            Assertions.assertEquals(3, p.replicas().size());
            Assertions.assertEquals(3, p.isr().size());
          });
    }
  }

  @Test
  void testPendingRequest() {
    var topic = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      Assertions.assertEquals(0, admin.pendingRequests());
    }
  }
}
