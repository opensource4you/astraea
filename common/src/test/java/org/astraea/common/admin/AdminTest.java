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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.astraea.common.DataRate;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class AdminTest extends RequireBrokerCluster {

  @CsvSource(value = {"1", "10", "100"})
  @ParameterizedTest
  void testWaitPartitionLeaderSynced(int partitions) {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(partitions).run();
      Assertions.assertTrue(
          admin
              .waitPartitionLeaderSynced(Map.of(topic, partitions), Duration.ofSeconds(5))
              .toCompletableFuture()
              .join());
    }
  }

  @Test
  void testWaitReplicasSynced() {
    var partitions = 10;
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitions)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      var broker = brokerIds().iterator().next();
      admin
          .moveToBrokers(
              IntStream.range(0, partitions)
                  .mapToObj(id -> TopicPartition.of(topic, id))
                  .collect(Collectors.toMap(tp -> tp, ignored -> List.of(broker))))
          .toCompletableFuture()
          .join();
      admin
          .waitReplicasSynced(
              IntStream.range(0, partitions)
                  .mapToObj(id -> TopicPartitionReplica.of(topic, id, broker))
                  .collect(Collectors.toSet()),
              Duration.ofSeconds(5))
          .toCompletableFuture()
          .join();
    }
  }

  @Test
  void testWaitPreferredLeaderSynced() {
    var partitions = 10;
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitions)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      var broker = brokerIds().iterator().next();
      var topicPartitions =
          IntStream.range(0, partitions)
              .mapToObj(id -> TopicPartition.of(topic, id))
              .collect(Collectors.toSet());

      admin
          .moveToBrokers(
              topicPartitions.stream()
                  .collect(Collectors.toMap(tp -> tp, ignored -> List.of(broker))))
          .toCompletableFuture()
          .join();

      admin
          .waitReplicasSynced(
              topicPartitions.stream()
                  .map(tp -> TopicPartitionReplica.of(tp.topic(), tp.partition(), broker))
                  .collect(Collectors.toSet()),
              Duration.ofSeconds(3))
          .toCompletableFuture()
          .join();

      admin.preferredLeaderElection(topicPartitions).toCompletableFuture().join();

      admin
          .waitPreferredLeaderSynced(topicPartitions, Duration.ofSeconds(5))
          .toCompletableFuture()
          .join();
    }
  }

  @Test
  void testClusterInfo() {
    try (var admin =
        new AdminImpl(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()))) {
      try (var producer = Producer.of(bootstrapServers())) {
        producer.send(Record.builder().topic(Utils.randomString()).key(new byte[100]).build());
        producer.send(Record.builder().topic(Utils.randomString()).key(new byte[55]).build());
        producer.send(Record.builder().topic(Utils.randomString()).key(new byte[33]).build());
      }

      try (var consumer =
          Consumer.forTopics(admin.topicNames(false).toCompletableFuture().join())
              .bootstrapServers(bootstrapServers())
              .config(
                  ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                  ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
              .build()) {
        Assertions.assertNotEquals(0, consumer.poll(3, Duration.ofSeconds(7)).size());
      }

      var topics =
          admin
              .topics(admin.topicNames(true).toCompletableFuture().join())
              .toCompletableFuture()
              .join();
      topics.forEach(
          t -> {
            var replicas =
                admin.clusterInfo(Set.of(t.name())).toCompletableFuture().join().replicas();
            Assertions.assertNotEquals(0, replicas.size());
            replicas.forEach(r -> Assertions.assertEquals(t.internal(), r.internal()));
            replicas.forEach(r -> Assertions.assertFalse(r.isAdding()));
            replicas.forEach(r -> Assertions.assertFalse(r.isRemoving()));
          });

      var clusterInfo =
          admin
              .clusterInfo(topics.stream().map(Topic::name).collect(Collectors.toUnmodifiableSet()))
              .toCompletableFuture()
              .join();

      Assertions.assertEquals(
          logFolders(),
          clusterInfo.brokerFolders(),
          "The log folder information is available from the admin version of ClusterInfo");
    }
  }

  @Test
  void testWaitClusterWithException() {

    try (var admin =
        new AdminImpl(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())) {
          @Override
          public CompletionStage<ClusterInfo<Replica>> clusterInfo(Set<String> topics) {
            return CompletableFuture.failedFuture(
                new org.apache.kafka.common.errors.UnknownTopicOrPartitionException());
          }
        }) {
      Assertions.assertFalse(
          admin
              .waitCluster(Set.of(), ignored -> true, Duration.ofSeconds(3), 2)
              .toCompletableFuture()
              .join());
    }
  }

  @Test
  void testWaitCluster() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(Utils.randomString()).run().toCompletableFuture().join();
      var topics = admin.topicNames(true).toCompletableFuture().join();
      var count = new AtomicInteger();

      admin
          .waitCluster(
              topics,
              ignored -> {
                count.incrementAndGet();
                return true;
              },
              Duration.ofSeconds(3),
              2)
          .toCompletableFuture()
          .join();
      Assertions.assertEquals(3, count.get());

      admin
          .waitCluster(
              topics,
              ignored -> {
                count.incrementAndGet();
                return true;
              },
              Duration.ofSeconds(3),
              1)
          .toCompletableFuture()
          .join();
      Assertions.assertEquals(5, count.get());

      Assertions.assertFalse(
          admin
              .waitCluster(topics, ignored -> false, Duration.ofSeconds(1), 0)
              .toCompletableFuture()
              .join());

      Assertions.assertTrue(
          admin
              .waitCluster(topics, ignored -> true, Duration.ofSeconds(1), 0)
              .toCompletableFuture()
              .join());

      // test timeout
      var count1 = new AtomicInteger();
      Assertions.assertFalse(
          admin
              .waitCluster(
                  topics,
                  ignored -> {
                    Utils.sleep(Duration.ofSeconds(3));
                    return count1.getAndIncrement() != 0;
                  },
                  Duration.ofMillis(300),
                  0)
              .toCompletableFuture()
              .join());
    }
  }

  @Test
  void testOrder() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(Utils.randomString()).run().toCompletableFuture().join();
      admin.creator().topic(Utils.randomString()).run().toCompletableFuture().join();
      admin.creator().topic(Utils.randomString()).run().toCompletableFuture().join();
      admin.creator().topic(Utils.randomString()).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));

      var topicNames = admin.topicNames(true).toCompletableFuture().join();
      Assertions.assertInstanceOf(SortedSet.class, topicNames);

      var topics = admin.topics(topicNames).toCompletableFuture().join();
      Assertions.assertEquals(
          topics.stream().sorted(Comparator.comparing(Topic::name)).collect(Collectors.toList()),
          topics);

      Assertions.assertInstanceOf(
          SortedSet.class, admin.topicPartitions(topicNames).toCompletableFuture().join());

      var partitions = admin.partitions(topicNames).toCompletableFuture().join();
      Assertions.assertEquals(
          partitions.stream()
              .sorted(Comparator.comparing(Partition::topic).thenComparing(Partition::partition))
              .collect(Collectors.toList()),
          partitions);

      Assertions.assertInstanceOf(
          SortedSet.class, admin.topicPartitionReplicas(brokerIds()).toCompletableFuture().join());

      Assertions.assertInstanceOf(SortedSet.class, admin.nodeInfos().toCompletableFuture().join());

      var brokers = admin.brokers().toCompletableFuture().join();
      Assertions.assertEquals(
          brokers.stream().sorted(Comparator.comparing(Broker::id)).collect(Collectors.toList()),
          brokers);

      var replicas = admin.clusterInfo(topicNames).toCompletableFuture().join().replicas();
      Assertions.assertEquals(
          replicas.stream()
              .sorted(
                  Comparator.comparing(Replica::topic)
                      .thenComparing(Replica::partition)
                      .thenComparing(r -> r.nodeInfo().id()))
              .collect(Collectors.toList()),
          replicas);
    }
  }

  @Test
  void testMoveLeaderBroker() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));

      var partitions = admin.partitions(Set.of(topic)).toCompletableFuture().join();
      Assertions.assertEquals(1, partitions.size());
      var partition = partitions.get(0);
      Assertions.assertEquals(1, partition.replicas().size());
      var ids =
          List.of(
              brokerIds().stream()
                  .filter(i -> i != partition.leader().get().id())
                  .findFirst()
                  .get(),
              partition.leader().get().id());
      admin.moveToBrokers(Map.of(TopicPartition.of(topic, 0), ids)).toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));

      var newPartitions = admin.partitions(Set.of(topic)).toCompletableFuture().join();
      Assertions.assertEquals(1, partitions.size());
      var newPartition = newPartitions.get(0);
      Assertions.assertEquals(ids.size(), newPartition.replicas().size());
      Assertions.assertEquals(ids.size(), newPartition.isr().size());
      Assertions.assertNotEquals(ids.get(0), newPartition.leader().get().id());

      admin
          .preferredLeaderElection(
              Set.of(TopicPartition.of(partition.topic(), partition.partition())))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          ids.get(0),
          admin.partitions(Set.of(topic)).toCompletableFuture().join().get(0).leader().get().id());
    }
  }

  @Test
  void testMoveToAnotherFolder() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      var replicas = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join().replicas();
      Assertions.assertEquals(1, replicas.size());

      var replica = replicas.get(0);
      var idAndFolder =
          logFolders().entrySet().stream()
              .filter(e -> e.getKey() == replica.nodeInfo().id())
              .map(e -> Map.of(e.getKey(), e.getValue().iterator().next()))
              .findFirst()
              .get();
      Assertions.assertEquals(1, idAndFolder.size());
      var id = idAndFolder.keySet().iterator().next();
      var path = idAndFolder.values().iterator().next();
      admin
          .moveToFolders(Map.of(TopicPartitionReplica.of(topic, 0, id), path))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      var newReplicas = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join().replicas();
      Assertions.assertEquals(1, newReplicas.size());

      var newReplica = newReplicas.get(0);
      Assertions.assertEquals(idAndFolder.get(newReplica.nodeInfo().id()), newReplica.path());
    }
  }

  @Test
  void testSetAndUnsetTopicConfig() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));

      admin
          .setTopicConfigs(Map.of(topic, Map.of(TopicConfigs.FILE_DELETE_DELAY_MS_CONFIG, "3000")))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      var config = admin.topics(Set.of(topic)).toCompletableFuture().join().get(0).config();
      Assertions.assertEquals("3000", config.value(TopicConfigs.FILE_DELETE_DELAY_MS_CONFIG).get());

      admin
          .unsetTopicConfigs(Map.of(topic, Set.of(TopicConfigs.FILE_DELETE_DELAY_MS_CONFIG)))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      config = admin.topics(Set.of(topic)).toCompletableFuture().join().get(0).config();
      Assertions.assertNotEquals(
          "3000", config.value(TopicConfigs.FILE_DELETE_DELAY_MS_CONFIG).get());
    }
  }

  @Test
  void testSetAndUnsetBrokerConfig() {
    try (var admin = Admin.of(bootstrapServers())) {
      var broker = admin.brokers().toCompletableFuture().join().get(0);
      var id = broker.id();
      Assertions.assertEquals("producer", broker.config().value("compression.type").get());

      admin
          .setBrokerConfigs(Map.of(id, Map.of(BrokerConfigs.COMPRESSION_TYPE_CONFIG, "gzip")))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      broker =
          admin.brokers().toCompletableFuture().join().stream()
              .filter(b -> b.id() == id)
              .findFirst()
              .get();
      Assertions.assertEquals(
          "gzip", broker.config().value(BrokerConfigs.COMPRESSION_TYPE_CONFIG).get());

      admin
          .unsetBrokerConfigs(Map.of(id, Set.of(BrokerConfigs.COMPRESSION_TYPE_CONFIG)))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      broker =
          admin.brokers().toCompletableFuture().join().stream()
              .filter(b -> b.id() == id)
              .findFirst()
              .get();
      Assertions.assertNotEquals(
          "gzip", broker.config().value(BrokerConfigs.COMPRESSION_TYPE_CONFIG).get());
    }
  }

  @Test
  void testMigrateToOtherBrokers() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().join();

      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          1, admin.clusterInfo(Set.of(topic)).toCompletableFuture().join().replicaStream().count());
      admin
          .moveToBrokers(Map.of(TopicPartition.of(topic, 0), new ArrayList<>(brokerIds())))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          3, admin.clusterInfo(Set.of(topic)).toCompletableFuture().join().replicaStream().count());

      admin
          .moveToBrokers(
              Map.of(TopicPartition.of(topic, 0), List.of(brokerIds().iterator().next())))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          1, admin.clusterInfo(Set.of(topic)).toCompletableFuture().join().replicaStream().count());
    }
  }

  @Test
  void testMigrateToOtherFolders() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          1, admin.clusterInfo(Set.of(topic)).toCompletableFuture().join().replicaStream().count());

      var id =
          admin
              .clusterInfo(Set.of(topic))
              .toCompletableFuture()
              .join()
              .replicaStream()
              .iterator()
              .next()
              .nodeInfo()
              .id();
      var paths = new ArrayList<>(logFolders().get(id));

      for (var path : paths) {
        admin
            .moveToFolders(Map.of(TopicPartitionReplica.of(topic, 0, id), path))
            .toCompletableFuture()
            .join();
        Utils.sleep(Duration.ofSeconds(2));
        Assertions.assertEquals(
            path,
            admin
                .clusterInfo(Set.of(topic))
                .toCompletableFuture()
                .join()
                .replicaStream()
                .iterator()
                .next()
                .path());
      }
    }
  }

  @Test
  void testCreator() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .configs(Map.of(TopicConfigs.COMPRESSION_TYPE_CONFIG, "lz4"))
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      var config = admin.topics(Set.of(topic)).toCompletableFuture().join().get(0).config();
      config.raw().keySet().forEach(key -> Assertions.assertTrue(config.value(key).isPresent()));
      Assertions.assertTrue(config.raw().containsValue("lz4"));
    }
  }

  @Test
  void testCreateTopicRepeatedly() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      Runnable createTopic =
          () ->
              admin
                  .creator()
                  .configs(Map.of(TopicConfigs.COMPRESSION_TYPE_CONFIG, "lz4"))
                  .numberOfReplicas((short) 1)
                  .numberOfPartitions(3)
                  .topic(topic)
                  .run()
                  .toCompletableFuture()
                  .join();

      createTopic.run();
      Utils.waitFor(
          () ->
              Utils.packException(
                  () -> admin.topicNames(true).toCompletableFuture().join().contains(topic)));
      IntStream.range(0, 10).forEach(i -> createTopic.run());

      // changing number of partitions can producer error
      Assertions.assertInstanceOf(
          IllegalArgumentException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      admin
                          .creator()
                          .numberOfPartitions(1)
                          .topic(topic)
                          .run()
                          .toCompletableFuture()
                          .join())
              .getCause());

      // changing number of replicas can producer error
      Assertions.assertInstanceOf(
          IllegalArgumentException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      admin
                          .creator()
                          .numberOfReplicas((short) 2)
                          .topic(topic)
                          .run()
                          .toCompletableFuture()
                          .join())
              .getCause());

      // changing config can producer error
      Assertions.assertInstanceOf(
          IllegalArgumentException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      admin
                          .creator()
                          .configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"))
                          .topic(topic)
                          .run()
                          .toCompletableFuture()
                          .join())
              .getCause());
    }
  }

  @Test
  void testPartitions() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      var before =
          brokerIds().stream()
              .mapToInt(
                  id ->
                      Utils.packException(
                          () ->
                              admin
                                  .topicPartitionReplicas(Set.of(id))
                                  .toCompletableFuture()
                                  .get()
                                  .size()))
              .sum();

      admin.creator().topic(topic).numberOfPartitions(10).run().toCompletableFuture().join();
      // wait for syncing topic creation
      Utils.sleep(Duration.ofSeconds(5));
      Assertions.assertTrue(admin.topicNames(true).toCompletableFuture().join().contains(topic));
      var partitions = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join().replicas();
      Assertions.assertEquals(10, partitions.size());
      var logFolders =
          logFolders().values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
      partitions.forEach(
          replica -> Assertions.assertTrue(logFolders.stream().anyMatch(replica.path()::contains)));
      brokerIds()
          .forEach(
              id ->
                  Assertions.assertNotEquals(
                      0,
                      admin
                          .topicPartitionReplicas(Set.of(id))
                          .toCompletableFuture()
                          .join()
                          .size()));

      var after =
          brokerIds().stream()
              .mapToInt(
                  id ->
                      admin.topicPartitionReplicas(Set.of(id)).toCompletableFuture().join().size())
              .sum();

      Assertions.assertEquals(before + 10, after);
    }
  }

  @Test
  void testOffsets() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(3).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      var partitions = admin.partitions(Set.of(topic)).toCompletableFuture().join();
      Assertions.assertEquals(3, partitions.size());
      partitions.forEach(
          p -> {
            Assertions.assertEquals(0, p.earliestOffset());
            Assertions.assertEquals(0, p.latestOffset());
          });
      try (var producer = Producer.of(bootstrapServers())) {
        producer.send(Record.builder().topic(topic).key(new byte[100]).partition(0).build());
        producer.send(Record.builder().topic(topic).key(new byte[55]).partition(1).build());
        producer.send(Record.builder().topic(topic).key(new byte[33]).partition(2).build());
      }

      admin
          .partitions(Set.of(topic))
          .toCompletableFuture()
          .join()
          .forEach(p -> Assertions.assertEquals(0, p.earliestOffset()));
      admin
          .partitions(Set.of(topic))
          .toCompletableFuture()
          .join()
          .forEach(p -> Assertions.assertEquals(1, p.latestOffset()));
      admin
          .partitions(Set.of(topic))
          .toCompletableFuture()
          .join()
          .forEach(p -> Assertions.assertNotEquals(Optional.empty(), p.maxTimestamp()));
    }
  }

  @Test
  void testConsumerGroups() {
    var topic = Utils.randomString();
    var consumerGroup = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(3).run().toCompletableFuture().join();
      try (var c1 =
          Consumer.forTopics(Set.of(topic))
              .bootstrapServers(bootstrapServers())
              .config(ConsumerConfigs.GROUP_ID_CONFIG, consumerGroup)
              .build()) {
        // wait for syncing topic creation
        Utils.sleep(Duration.ofSeconds(5));
        var consumerGroupMap =
            admin.consumerGroups(Set.of(consumerGroup)).toCompletableFuture().join();
        Assertions.assertEquals(1, consumerGroupMap.size());
        Assertions.assertTrue(
            consumerGroupMap.stream().anyMatch(cg -> cg.groupId().equals(consumerGroup)));

        try (var c2 =
            Consumer.forTopics(Set.of(topic))
                .bootstrapServers(bootstrapServers())
                .config(ConsumerConfigs.GROUP_ID_CONFIG, "abc")
                .build()) {
          var count =
              admin.consumerGroupIds().toCompletableFuture().join().stream()
                  .mapToInt(
                      t -> admin.consumerGroups(Set.of(t)).toCompletableFuture().join().size())
                  .sum();
          Assertions.assertEquals(
              count,
              admin
                  .consumerGroups(admin.consumerGroupIds().toCompletableFuture().join())
                  .toCompletableFuture()
                  .join()
                  .size());
          Assertions.assertEquals(
              1, admin.consumerGroups(Set.of("abc")).toCompletableFuture().join().size());

          // test internal
          Assertions.assertNotEquals(
              0,
              admin
                  .topics(admin.topicNames(true).toCompletableFuture().join())
                  .toCompletableFuture()
                  .join()
                  .stream()
                  .filter(Topic::internal)
                  .count());
          Assertions.assertNotEquals(
              0,
              admin
                  .partitions(admin.topicNames(true).toCompletableFuture().join())
                  .toCompletableFuture()
                  .join()
                  .stream()
                  .filter(Partition::internal)
                  .count());

          // test internal topics
          Assertions.assertNotEquals(
              0, admin.internalTopicNames().toCompletableFuture().join().size());
        }
      }
    }
  }

  @Test
  void testMigrateSinglePartition() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().join();

      Utils.sleep(Duration.ofSeconds(5));
      var broker = brokerIds().iterator().next();
      admin.moveToBrokers(Map.of(TopicPartition.of(topic, 0), List.of(broker)));

      Utils.waitFor(
          () -> {
            var partitionReplicas =
                admin.clusterInfo(Set.of(topic)).toCompletableFuture().join().replicas();
            return partitionReplicas.size() == 1
                && partitionReplicas.get(0).nodeInfo().id() == broker;
          });

      var currentBroker =
          admin
              .clusterInfo(Set.of(topic))
              .toCompletableFuture()
              .join()
              .replicaStream()
              .filter(replica -> replica.partition() == 0)
              .findFirst()
              .get()
              .nodeInfo()
              .id();
      var allPath = admin.brokerFolders().toCompletableFuture().join();
      var otherPath =
          allPath.get(currentBroker).stream()
              .filter(
                  i ->
                      !i.contains(
                          admin
                              .clusterInfo(Set.of(topic))
                              .toCompletableFuture()
                              .join()
                              .replicaStream()
                              .filter(replica -> replica.partition() == 0)
                              .findFirst()
                              .get()
                              .path()))
              .collect(Collectors.toSet());

      admin.moveToFolders(
          Map.of(TopicPartitionReplica.of(topic, 0, currentBroker), otherPath.iterator().next()));
      Utils.waitFor(
          () -> {
            var partitionReplicas =
                admin.clusterInfo(Set.of(topic)).toCompletableFuture().join().replicas();
            return partitionReplicas.size() == 1
                && partitionReplicas.get(0).path().equals(otherPath.iterator().next());
          });
    }
  }

  @Test
  void testIllegalMigrationArgument() {
    var topic = Utils.randomString();
    var topicParition = TopicPartition.of(topic, 0);
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      var currentReplica =
          admin
              .clusterInfo(Set.of(topic))
              .toCompletableFuture()
              .join()
              .replicaStream()
              .filter(replica -> replica.partition() == topicParition.partition())
              .findFirst()
              .get();

      var currentBroker = currentReplica.nodeInfo().id();
      var notExistReplica = (currentBroker + 1) % brokerIds().size();
      var nextDir = logFolders().get(notExistReplica).iterator().next();

      Assertions.assertThrows(
          CompletionException.class,
          () ->
              admin
                  .moveToFolders(Map.of(TopicPartitionReplica.of(topic, 0, currentBroker), nextDir))
                  .toCompletableFuture()
                  .join());
    }
  }

  @Test
  void testMigrateAllPartitions() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(3).run().toCompletableFuture().join();

      Utils.sleep(Duration.ofSeconds(5));
      var broker = brokerIds().iterator().next();
      admin
          .partitions(Set.of(topic))
          .toCompletableFuture()
          .join()
          .forEach(p -> admin.moveToBrokers(Map.of(p.topicPartition(), List.of(broker))));
      Utils.waitFor(
          () -> {
            var replicas = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join().replicas();
            return replicas.stream().allMatch(r -> r.nodeInfo().id() == broker);
          });
    }
  }

  @Test
  void testReplicaSize() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      producer
          .send(Record.builder().topic(topic).key(new byte[100]).build())
          .toCompletableFuture()
          .join();
      var originSize =
          admin
              .clusterInfo(Set.of(topic))
              .thenApply(
                  replicas ->
                      replicas
                          .replicaStream()
                          .filter(replica -> replica.partition() == 0)
                          .findFirst()
                          .get()
                          .size())
              .toCompletableFuture()
              .join();

      // add data again
      producer
          .send(Record.builder().topic(topic).key(new byte[100]).build())
          .toCompletableFuture()
          .join();

      var newSize =
          admin
              .clusterInfo(Set.of(topic))
              .thenApply(
                  replicas ->
                      replicas
                          .replicaStream()
                          .filter(replica -> replica.partition() == 0)
                          .findFirst()
                          .get()
                          .size())
              .toCompletableFuture()
              .join();
      Assertions.assertTrue(newSize > originSize);
    }
  }

  @Test
  void testCompact() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .configs(
              Map.of(
                  TopicConfigs.MAX_COMPACTION_LAG_MS_CONFIG,
                  "1000",
                  TopicConfigs.CLEANUP_POLICY_CONFIG,
                  TopicConfigs.CLEANUP_POLICY_COMPACT))
          .run()
          .toCompletableFuture()
          .join();

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
            .forEach(
                i -> producer.send(Record.builder().key(key).value(value).topic(topic).build()));
        producer.flush();

        Utils.sleep(Duration.ofSeconds(2));
        IntStream.range(0, 10)
            .forEach(
                i ->
                    producer.send(
                        Record.builder().key(anotherKey).value(value).topic(topic).build()));
        producer.flush();
      }

      Utils.sleep(Duration.ofSeconds(3));

      try (var consumer =
          Consumer.forTopics(Set.of(topic))
              .keyDeserializer(Deserializer.STRING)
              .valueDeserializer(Deserializer.STRING)
              .config(
                  ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                  ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
              .bootstrapServers(bootstrapServers())
              .build()) {

        var records =
            IntStream.range(0, 5)
                .mapToObj(i -> consumer.poll(Duration.ofSeconds(1)))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        Assertions.assertEquals(
            1, records.stream().filter(record -> record.key().equals(key)).count());

        Assertions.assertEquals(
            10, records.stream().filter(record -> record.key().equals(anotherKey)).count());
      }
    }
  }

  @Test
  void testBrokers() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(6)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      var brokers = admin.brokers().toCompletableFuture().join();
      Assertions.assertEquals(3, brokers.size());
      brokers.forEach(broker -> Assertions.assertNotEquals(0, broker.config().raw().size()));
      Assertions.assertEquals(1, brokers.stream().filter(Broker::isController).count());
      brokers.forEach(broker -> Assertions.assertNotEquals(0, broker.topicPartitions().size()));
      brokers.forEach(
          broker -> Assertions.assertNotEquals(0, broker.topicPartitionLeaders().size()));
    }
  }

  @Test
  void testBrokerFolders() {
    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertEquals(
          brokerIds().size(), admin.brokers().toCompletableFuture().join().size());
      // list all
      logFolders()
          .forEach(
              (id, ds) ->
                  Assertions.assertEquals(
                      admin.brokerFolders().toCompletableFuture().join().get(id).size(),
                      ds.size()));

      admin
          .brokers()
          .toCompletableFuture()
          .join()
          .forEach(
              broker ->
                  Assertions.assertEquals(
                      broker.topicPartitions().size(),
                      broker.dataFolders().stream()
                          .mapToInt(e -> e.partitionSizes().size())
                          .sum()));
    }
  }

  @Test
  void testReplicas() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(2).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          2,
          admin
              .clusterInfo(Set.of(topic))
              .toCompletableFuture()
              .join()
              .replicaStream()
              .collect(
                  Collectors.groupingBy(
                      replica -> TopicPartition.of(replica.topic(), replica.partition())))
              .size());

      var count =
          admin
              .clusterInfo(admin.topicNames(true).toCompletableFuture().join())
              .toCompletableFuture()
              .join()
              .replicaStream()
              .collect(
                  Collectors.groupingBy(
                      replica -> TopicPartition.of(replica.topic(), replica.partition())))
              .size();
      Assertions.assertEquals(
          count,
          admin
              .clusterInfo(admin.topicNames(true).toCompletableFuture().join())
              .toCompletableFuture()
              .join()
              .replicaStream()
              .collect(
                  Collectors.groupingBy(
                      replica -> TopicPartition.of(replica.topic(), replica.partition())))
              .size());
    }
  }

  @Test
  void testReplicasPreferredLeaderFlag() {
    var topic = Utils.randomString();
    var partitionCount = 10;
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitionCount)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();

      Utils.sleep(Duration.ofSeconds(3));

      var expectedPreferredLeader =
          IntStream.range(0, partitionCount)
              .mapToObj(p -> TopicPartition.of(topic, p))
              .collect(Collectors.toUnmodifiableMap(p -> p, p -> List.of(0)));

      var currentPreferredLeader =
          (Supplier<Map<TopicPartition, List<Integer>>>)
              () ->
                  admin
                      .clusterInfo(Set.of(topic))
                      .toCompletableFuture()
                      .join()
                      .replicaStream()
                      .filter(Replica::isPreferredLeader)
                      .collect(
                          Collectors.groupingBy(
                              replica -> TopicPartition.of(replica.topic(), replica.partition()),
                              Collectors.mapping(
                                  replica -> replica.nodeInfo().id(), Collectors.toList())));

      IntStream.range(0, partitionCount)
          .forEach(p -> admin.moveToBrokers(Map.of(TopicPartition.of(topic, p), List.of(0, 1, 2))));
      Utils.sleep(Duration.ofSeconds(3));

      Assertions.assertEquals(expectedPreferredLeader, currentPreferredLeader.get());
    }
  }

  @Test
  void testProducerStates() {
    var topic = Utils.randomString();
    try (var producer = Producer.of(bootstrapServers());
        var admin = Admin.of(bootstrapServers())) {
      producer
          .send(Record.builder().topic(topic).value(new byte[1]).build())
          .toCompletableFuture()
          .join();

      var states =
          admin
              .producerStates(admin.topicPartitions(Set.of(topic)).toCompletableFuture().join())
              .toCompletableFuture()
              .join();
      Assertions.assertNotEquals(0, states.size());
      var producerState =
          states.stream()
              .filter(s -> s.topic().equals(topic))
              .collect(Collectors.toUnmodifiableList());
      Assertions.assertEquals(1, producerState.size());
    }
  }

  @Test
  void testConnectionQuotas() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin.setConnectionQuotas(Map.of(Utils.hostname(), 100)).toCompletableFuture().join();

      var quotas =
          admin.quotas(Set.of(QuotaConfigs.IP)).toCompletableFuture().join().stream()
              .filter(q -> q.targetValue().equals(Utils.hostname()))
              .collect(Collectors.toList());
      Assertions.assertNotEquals(0, quotas.size());
      quotas.forEach(
          quota -> {
            Assertions.assertEquals(Utils.hostname(), quota.targetValue());
            Assertions.assertEquals(QuotaConfigs.IP_CONNECTION_RATE_CONFIG, quota.limitKey());
            Assertions.assertEquals(100D, quota.limitValue());
          });

      admin.unsetConnectionQuotas(Set.of(Utils.hostname())).toCompletableFuture().join();
      Assertions.assertEquals(
          0,
          (int)
              admin.quotas().toCompletableFuture().join().stream()
                  .filter(q -> q.targetKey().equals(QuotaConfigs.IP))
                  .filter(q -> q.targetValue().equals(Utils.hostname()))
                  .filter(q -> q.limitKey().equals(QuotaConfigs.IP_CONNECTION_RATE_CONFIG))
                  .count());
    }
  }

  @Test
  void testProducerQuotas() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .setProducerQuotas(Map.of(Utils.hostname(), DataRate.Byte.of(100).perSecond()))
          .toCompletableFuture()
          .join();

      var quotas =
          admin.quotas(Set.of(QuotaConfigs.CLIENT_ID)).toCompletableFuture().join().stream()
              .filter(q -> q.targetValue().equals(Utils.hostname()))
              .filter(q -> q.limitKey().equals(QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG))
              .collect(Collectors.toList());
      Assertions.assertNotEquals(0, quotas.size());
      quotas.forEach(
          quota ->
              Assertions.assertEquals(
                  DataRate.Byte.of(100).perSecond().byteRate(), quota.limitValue()));

      admin.unsetProducerQuotas(Set.of(Utils.hostname())).toCompletableFuture().join();
      Assertions.assertEquals(
          0,
          (int)
              admin.quotas().toCompletableFuture().join().stream()
                  .filter(q -> q.targetKey().equals(QuotaConfigs.CLIENT_ID))
                  .filter(q -> q.targetValue().equals(Utils.hostname()))
                  .filter(q -> q.limitKey().equals(QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG))
                  .count());
    }
  }

  @Test
  void testConsumerQuotas() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .setConsumerQuotas(Map.of(Utils.hostname(), DataRate.Byte.of(1000).perSecond()))
          .toCompletableFuture()
          .join();

      var quotas =
          admin.quotas(Set.of(QuotaConfigs.CLIENT_ID)).toCompletableFuture().join().stream()
              .filter(q -> q.targetValue().equals(Utils.hostname()))
              .filter(q -> q.limitKey().equals(QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG))
              .collect(Collectors.toList());
      Assertions.assertNotEquals(0, quotas.size());
      quotas.forEach(
          quota ->
              Assertions.assertEquals(
                  DataRate.Byte.of(1000).perSecond().byteRate(), quota.limitValue()));

      admin.unsetConsumerQuotas(Set.of(Utils.hostname())).toCompletableFuture().join();
      Assertions.assertEquals(
          0,
          (int)
              admin.quotas().toCompletableFuture().join().stream()
                  .filter(q -> q.targetKey().equals(QuotaConfigs.CLIENT_ID))
                  .filter(q -> q.targetValue().equals(Utils.hostname()))
                  .filter(q -> q.limitKey().equals(QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG))
                  .count());
    }
  }

  @Test
  void testSizeOfNoDataTopic() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));

      admin
          .brokers()
          .toCompletableFuture()
          .join()
          .forEach(
              broker ->
                  broker
                      .dataFolders()
                      .forEach(
                          d ->
                              d.partitionSizes().entrySet().stream()
                                  .filter(e -> e.getKey().topic().equals(topic))
                                  .map(Map.Entry::getValue)
                                  .forEach(v -> Assertions.assertEquals(0, v))));
    }
  }

  @Test
  void testDeleteRecord() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      var deleteRecords =
          admin.deleteRecords(Map.of(TopicPartition.of(topic, 0), 0L)).toCompletableFuture().join();

      Assertions.assertEquals(1, deleteRecords.size());
      Assertions.assertEquals(0, deleteRecords.values().stream().findFirst().get());

      try (var producer = Producer.of(bootstrapServers())) {
        var records =
            Stream.of(0, 0, 0, 1, 1)
                .map(x -> Record.builder().topic(topic).partition(x).value(new byte[100]).build())
                .collect(Collectors.toList());
        producer.send(records);
        producer.flush();
      }

      deleteRecords =
          admin
              .deleteRecords(
                  Map.of(TopicPartition.of(topic, 0), 2L, TopicPartition.of(topic, 1), 1L))
              .toCompletableFuture()
              .join();
      Assertions.assertEquals(2, deleteRecords.size());
      Assertions.assertEquals(2, deleteRecords.get(TopicPartition.of(topic, 0)));
      Assertions.assertEquals(1, deleteRecords.get(TopicPartition.of(topic, 1)));

      var partitions = admin.partitions(Set.of(topic)).toCompletableFuture().join();
      Assertions.assertEquals(3, partitions.size());
      Assertions.assertEquals(
          2,
          partitions.stream()
              .filter(p -> p.topic().equals(topic) && p.partition() == 0)
              .findFirst()
              .get()
              .earliestOffset());
      Assertions.assertEquals(
          1,
          partitions.stream()
              .filter(p -> p.topic().equals(topic) && p.partition() == 1)
              .findFirst()
              .get()
              .earliestOffset());
      Assertions.assertEquals(
          0,
          partitions.stream()
              .filter(p -> p.topic().equals(topic) && p.partition() == 2)
              .findFirst()
              .get()
              .earliestOffset());
    }
  }

  @Test
  void testDeleteTopic() {
    var topic =
        IntStream.range(0, 4).mapToObj(x -> Utils.randomString()).collect(Collectors.toList());

    try (var admin = Admin.of(bootstrapServers())) {
      topic.forEach(
          x ->
              admin
                  .creator()
                  .topic(x)
                  .numberOfPartitions(3)
                  .numberOfReplicas((short) 3)
                  .run()
                  .toCompletableFuture()
                  .join());
      Utils.sleep(Duration.ofSeconds(2));

      admin.deleteTopics(Set.of(topic.get(0), topic.get(1)));
      Utils.sleep(Duration.ofSeconds(2));

      var latestTopicNames = admin.topicNames(true).toCompletableFuture().join();
      Assertions.assertFalse(latestTopicNames.contains(topic.get(0)));
      Assertions.assertFalse(latestTopicNames.contains(topic.get(1)));
      Assertions.assertTrue(latestTopicNames.contains(topic.get(2)));
      Assertions.assertTrue(latestTopicNames.contains(topic.get(3)));

      admin.deleteTopics(Set.of(topic.get(3)));
      Utils.sleep(Duration.ofSeconds(2));

      latestTopicNames = admin.topicNames(true).toCompletableFuture().join();
      Assertions.assertFalse(latestTopicNames.contains(topic.get(3)));
      Assertions.assertTrue(latestTopicNames.contains(topic.get(2)));
    }
  }

  @Test
  void testListPartitions() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(2)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      var partitions = admin.partitions(Set.of(topic)).toCompletableFuture().join();
      Assertions.assertEquals(2, partitions.size());
      partitions.forEach(
          p -> {
            Assertions.assertEquals(3, p.replicas().size());
            Assertions.assertEquals(3, p.isr().size());
          });
    }
  }

  @Timeout(5)
  @Test
  void testRunningRequests() {
    try (AdminImpl admin = (AdminImpl) Admin.of(bootstrapServers())) {
      Assertions.assertEquals(0, admin.runningRequests());
      var f0 = new KafkaFutureImpl<Integer>();
      var f1 = admin.to(f0);
      Assertions.assertEquals(1, admin.runningRequests());
      f0.complete(10);
      Assertions.assertEquals(0, admin.runningRequests());
      Assertions.assertEquals(10, f1.toCompletableFuture().join());
    }
  }

  @Test
  void testDeleteMembers() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      try (var producer = Producer.of(bootstrapServers())) {
        producer
            .send(Record.builder().topic(topic).key(new byte[10]).build())
            .toCompletableFuture()
            .join();
      }
      String groupId = null;
      try (var consumer =
          Consumer.forTopics(Set.of(topic))
              .bootstrapServers(bootstrapServers())
              .config(
                  ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                  ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
              .build()) {
        Assertions.assertEquals(1, consumer.poll(1, Duration.ofSeconds(5)).size());
        admin.deleteMembers(Set.of(consumer.groupId())).toCompletableFuture().join();
        groupId = consumer.groupId();
      }
      admin.deleteMembers(Set.of(groupId)).toCompletableFuture().join();

      var gs = admin.consumerGroups(Set.of(groupId)).toCompletableFuture().join();
      Assertions.assertEquals(1, gs.size());
      Assertions.assertEquals(0, gs.get(0).assignment().size());
    }
  }

  @Test
  void testDeleteGroups() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      try (var producer = Producer.of(bootstrapServers())) {
        producer
            .send(Record.builder().topic(topic).key(new byte[10]).build())
            .toCompletableFuture()
            .join();
      }
      String groupId = null;
      try (var consumer =
          Consumer.forTopics(Set.of(topic))
              .bootstrapServers(bootstrapServers())
              .config(
                  ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                  ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
              .build()) {
        Assertions.assertEquals(1, consumer.poll(1, Duration.ofSeconds(5)).size());
        groupId = consumer.groupId();
      }
      admin.deleteGroups(Set.of(groupId)).toCompletableFuture().join();
      Assertions.assertFalse(
          admin.consumerGroupIds().toCompletableFuture().join().contains(groupId));
    }
  }

  @Test
  void testRemoveStaticMembers() {
    var groupId = Utils.randomString(10);
    var topicName = Utils.randomString(10);
    var staticMember = Utils.randomString(10);
    try (var consumer =
        Consumer.forTopics(Set.of(topicName))
            .bootstrapServers(bootstrapServers())
            .config(ConsumerConfigs.GROUP_ID_CONFIG, groupId)
            .config(ConsumerConfigs.GROUP_INSTANCE_ID_CONFIG, staticMember)
            .build()) {
      Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
      try (var admin = Admin.of(bootstrapServers())) {
        Assertions.assertEquals(
            1,
            admin
                .consumerGroups(Set.of(groupId))
                .toCompletableFuture()
                .join()
                .get(0)
                .assignment()
                .keySet()
                .stream()
                .filter(m -> m.groupInstanceId().map(id -> id.equals(staticMember)).isPresent())
                .count());
        admin
            .deleteInstanceMembers(Map.of(groupId, Set.of(staticMember)))
            .toCompletableFuture()
            .join();
        Assertions.assertEquals(
            0,
            admin
                .consumerGroups(Set.of(groupId))
                .toCompletableFuture()
                .join()
                .get(0)
                .assignment()
                .keySet()
                .stream()
                .filter(m -> m.groupInstanceId().map(id -> id.equals(staticMember)).isPresent())
                .count());
      }
    }
  }

  @Test
  void testQueryNonexistentGroup() {
    var group = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      var groups = admin.consumerGroups(Set.of(group)).toCompletableFuture().join();
      Assertions.assertEquals(1, groups.size());
      Assertions.assertEquals(0, groups.get(0).assignment().size());
      Assertions.assertEquals(0, groups.get(0).consumeProgress().size());
    }
  }

  @Test
  void testPreferredLeaders() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      admin
          .preferredLeaderElection(Set.of(TopicPartition.of(topic, 0)))
          .toCompletableFuture()
          .join();
    }
  }

  @Test
  public void testTimestampOfLatestRecords() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(4).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));
      var ps = admin.topicPartitions(Set.of(topic)).toCompletableFuture().join();
      Assertions.assertEquals(
          0,
          admin
              .timestampOfLatestRecords(ps, Duration.ofSeconds(3))
              .toCompletableFuture()
              .join()
              .size());

      var t = System.currentTimeMillis();
      try (var producer = Producer.of(bootstrapServers())) {
        producer.send(
            Record.builder()
                .topic(topic)
                .partition(0)
                .timestamp(t)
                .key(Utils.randomString().getBytes(StandardCharsets.UTF_8))
                .build());
        producer.send(
            Record.builder()
                .topic(topic)
                .partition(1)
                .timestamp(t + 1)
                .key(Utils.randomString().getBytes(StandardCharsets.UTF_8))
                .build());
        producer.send(
            Record.builder()
                .topic(topic)
                .partition(2)
                .timestamp(t + 2)
                .key(Utils.randomString().getBytes(StandardCharsets.UTF_8))
                .build());
        producer.send(
            Record.builder()
                .topic(topic)
                .partition(3)
                .timestamp(t + 3)
                .key(Utils.randomString().getBytes(StandardCharsets.UTF_8))
                .build());
        producer.flush();
      }
      var latestRecords =
          admin.latestRecords(ps, 4, Duration.ofSeconds(3)).toCompletableFuture().join();
      Assertions.assertEquals(4, latestRecords.size());
      latestRecords.values().forEach(v -> Assertions.assertEquals(1, v.size()));

      var ts =
          admin.timestampOfLatestRecords(ps, Duration.ofSeconds(6)).toCompletableFuture().join();
      Assertions.assertEquals(4, ts.size());
      Assertions.assertEquals(t, ts.get(TopicPartition.of(topic, 0)));
      Assertions.assertEquals(t + 1, ts.get(TopicPartition.of(topic, 1)));
      Assertions.assertEquals(t + 2, ts.get(TopicPartition.of(topic, 2)));
      Assertions.assertEquals(t + 3, ts.get(TopicPartition.of(topic, 3)));
    }
  }

  @Test
  void testAppendConfigsToTopic() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();

      Utils.sleep(Duration.ofSeconds(3));

      // append to nonexistent value
      Assertions.assertEquals(
          Optional.empty(),
          admin
              .topics(Set.of(topic))
              .toCompletableFuture()
              .join()
              .get(0)
              .config()
              .value(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG));
      admin
          .appendTopicConfigs(
              Map.of(
                  topic,
                  Map.of(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:1001")))
          .toCompletableFuture()
          .join();
      Assertions.assertEquals(
          "1:1001",
          admin
              .topics(Set.of(topic))
              .toCompletableFuture()
              .join()
              .get(0)
              .config()
              .value(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
              .get());

      // append to existent value
      admin
          .appendTopicConfigs(
              Map.of(
                  topic,
                  Map.of(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "2:1002")))
          .toCompletableFuture()
          .join();
      Assertions.assertEquals(
          "1:1001,2:1002",
          admin
              .topics(Set.of(topic))
              .toCompletableFuture()
              .join()
              .get(0)
              .config()
              .value(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
              .get());

      // append duplicate value
      admin
          .appendTopicConfigs(
              Map.of(
                  topic,
                  Map.of(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "2:1002")))
          .toCompletableFuture()
          .join();
      Assertions.assertEquals(
          "1:1001,2:1002",
          admin
              .topics(Set.of(topic))
              .toCompletableFuture()
              .join()
              .get(0)
              .config()
              .value(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
              .get());

      // append to wildcard
      admin
          .setTopicConfigs(
              Map.of(
                  topic, Map.of(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "*")))
          .toCompletableFuture()
          .join();
      Assertions.assertEquals(
          "*",
          admin
              .topics(Set.of(topic))
              .toCompletableFuture()
              .join()
              .get(0)
              .config()
              .value(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
              .get());
      admin
          .appendTopicConfigs(
              Map.of(
                  topic,
                  Map.of(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:1001")))
          .toCompletableFuture()
          .join();
      Assertions.assertEquals(
          "*",
          admin
              .topics(Set.of(topic))
              .toCompletableFuture()
              .join()
              .get(0)
              .config()
              .value(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
              .get());
    }
  }

  @Test
  void testSubtractTopicConfigs() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();

      Utils.sleep(Duration.ofSeconds(3));

      // subtract existent value
      admin
          .setTopicConfigs(
              Map.of(
                  topic,
                  Map.of(
                      TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
                      "1:1001,2:1003")))
          .toCompletableFuture()
          .join();
      admin
          .subtractTopicConfigs(
              Map.of(
                  topic,
                  Map.of(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:1001")))
          .toCompletableFuture()
          .join();
      Assertions.assertEquals(
          "2:1003",
          admin
              .topics(Set.of(topic))
              .toCompletableFuture()
              .join()
              .get(0)
              .config()
              .value(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
              .get());

      // subtract nonexistent value
      admin
          .subtractTopicConfigs(
              Map.of(
                  topic,
                  Map.of(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:1001")))
          .toCompletableFuture()
          .join();
      Assertions.assertEquals(
          "2:1003",
          admin
              .topics(Set.of(topic))
              .toCompletableFuture()
              .join()
              .get(0)
              .config()
              .value(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
              .get());

      // can't subtract *
      admin
          .setTopicConfigs(
              Map.of(
                  topic, Map.of(TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "*")))
          .toCompletableFuture()
          .join();

      Assertions.assertInstanceOf(
          IllegalArgumentException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      admin
                          .subtractTopicConfigs(
                              Map.of(
                                  topic,
                                  Map.of(
                                      TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
                                      "1:1001")))
                          .toCompletableFuture()
                          .join())
              .getCause());
    }
  }

  @Test
  void testValueOfConfigs() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .topics(admin.topicNames(true).toCompletableFuture().join())
          .toCompletableFuture()
          .join()
          .forEach(
              t ->
                  t.config()
                      .raw()
                      .forEach(
                          (k, v) -> Assertions.assertNotEquals(0, v.trim().length(), "key: " + k)));
      admin
          .brokers()
          .toCompletableFuture()
          .join()
          .forEach(
              t ->
                  t.config()
                      .raw()
                      .forEach(
                          (k, v) -> Assertions.assertNotEquals(0, v.trim().length(), "key: " + k)));
    }
  }

  @Test
  void testDynamicTopicConfig() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();

      Utils.sleep(Duration.ofSeconds(3));

      var sets =
          admin
              .topics(Set.of(topic))
              .toCompletableFuture()
              .join()
              .get(0)
              .config()
              .raw()
              .entrySet()
              .stream()
              .filter(entry -> TopicConfigs.ALL_CONFIGS.contains(entry.getKey()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      admin.setTopicConfigs(Map.of(topic, sets)).toCompletableFuture().join();
    }
  }

  @Test
  void testDynamicBrokerConfig() {
    try (var admin = Admin.of(bootstrapServers())) {
      var broker = admin.brokers().toCompletableFuture().join().get(0);
      var sets =
          broker.config().raw().entrySet().stream()
              .filter(entry -> BrokerConfigs.DYNAMICAL_CONFIGS.contains(entry.getKey()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      admin.setBrokerConfigs(Map.of(broker.id(), sets)).toCompletableFuture().join();
    }
  }

  @Test
  void testBootstrapServers() {
    try (var admin = Admin.of(bootstrapServers())) {
      var bootstrapServers =
          admin
              .brokers()
              .thenApply(
                  brokers ->
                      brokers.stream()
                          .map(broker -> broker.host() + ":" + broker.port())
                          .collect(Collectors.toList()))
              .toCompletableFuture()
              .join();
      admin
          .bootstrapServers()
          .thenAccept(
              bs ->
                  Arrays.stream(bs.split(","))
                      .forEach(
                          bootstrapServer ->
                              Assertions.assertTrue(bootstrapServers.contains(bootstrapServer))));
    }
  }
}
