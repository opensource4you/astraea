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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
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
import org.astraea.common.producer.Serializer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class AsyncAdminTest extends RequireBrokerCluster {

  @CsvSource(value = {"1", "10", "100"})
  @ParameterizedTest
  void testWaitPartitionLeaderSynced(int partitions)
      throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(partitions).run();
      Assertions.assertTrue(
          admin
              .waitPartitionLeaderSynced(Map.of(topic, partitions), Duration.ofSeconds(5))
              .toCompletableFuture()
              .get());
    }
  }

  @Test
  void testWaitReplicasSynced() throws ExecutionException, InterruptedException {
    var partitions = 10;
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(partitions).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));

      var broker = brokerIds().iterator().next();
      admin
          .moveToBrokers(
              IntStream.range(0, partitions)
                  .mapToObj(id -> TopicPartition.of(topic, id))
                  .collect(Collectors.toMap(tp -> tp, ignored -> List.of(broker))))
          .toCompletableFuture()
          .get();
      admin
          .waitReplicasSynced(
              IntStream.range(0, partitions)
                  .mapToObj(id -> TopicPartitionReplica.of(topic, id, broker))
                  .collect(Collectors.toSet()),
              Duration.ofSeconds(5))
          .toCompletableFuture()
          .get();
    }
  }

  @Test
  void testWaitPreferredLeaderSynced() throws ExecutionException, InterruptedException {
    var partitions = 10;
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(partitions).run().toCompletableFuture().get();
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
          .get();

      admin
          .waitReplicasSynced(
              topicPartitions.stream()
                  .map(tp -> TopicPartitionReplica.of(tp.topic(), tp.partition(), broker))
                  .collect(Collectors.toSet()),
              Duration.ofSeconds(3))
          .toCompletableFuture()
          .get();

      admin.preferredLeaderElection(topicPartitions).toCompletableFuture().get();

      admin
          .waitPreferredLeaderSynced(topicPartitions, Duration.ofSeconds(5))
          .toCompletableFuture()
          .get();
    }
  }

  @Test
  void testWaitClusterWithException() throws ExecutionException, InterruptedException {

    try (var admin =
        new AsyncAdminImpl(
            Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())) {
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
              .get());
    }
  }

  @Test
  void testWaitCluster() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(Utils.randomString()).run().toCompletableFuture().get();
      var topics = admin.topicNames(true).toCompletableFuture().get();
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
          .get();
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
          .get();
      Assertions.assertEquals(5, count.get());

      Assertions.assertFalse(
          admin
              .waitCluster(topics, ignored -> false, Duration.ofSeconds(1), 0)
              .toCompletableFuture()
              .get());

      Assertions.assertTrue(
          admin
              .waitCluster(topics, ignored -> true, Duration.ofSeconds(1), 0)
              .toCompletableFuture()
              .get());

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
              .get());
    }
  }

  @Test
  void testOrder() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(Utils.randomString()).run().toCompletableFuture().get();
      admin.creator().topic(Utils.randomString()).run().toCompletableFuture().get();
      admin.creator().topic(Utils.randomString()).run().toCompletableFuture().get();
      admin.creator().topic(Utils.randomString()).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(3));

      var topicNames = admin.topicNames(true).toCompletableFuture().get();
      Assertions.assertInstanceOf(SortedSet.class, topicNames);

      var topics = admin.topics(topicNames).toCompletableFuture().get();
      Assertions.assertEquals(
          topics.stream().sorted(Comparator.comparing(Topic::name)).collect(Collectors.toList()),
          topics);

      Assertions.assertInstanceOf(
          SortedSet.class, admin.topicPartitions(topicNames).toCompletableFuture().get());

      var partitions = admin.partitions(topicNames).toCompletableFuture().get();
      Assertions.assertEquals(
          partitions.stream()
              .sorted(Comparator.comparing(Partition::topic).thenComparing(Partition::partition))
              .collect(Collectors.toList()),
          partitions);

      Assertions.assertInstanceOf(
          SortedSet.class, admin.topicPartitionReplicas(brokerIds()).toCompletableFuture().get());

      Assertions.assertInstanceOf(SortedSet.class, admin.nodeInfos().toCompletableFuture().get());

      var brokers = admin.brokers().toCompletableFuture().get();
      Assertions.assertEquals(
          brokers.stream().sorted(Comparator.comparing(Broker::id)).collect(Collectors.toList()),
          brokers);

      var replicas = admin.replicas(topicNames).toCompletableFuture().get();
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
  void testMoveLeaderBroker() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));

      var partitions = admin.partitions(Set.of(topic)).toCompletableFuture().get();
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
      admin.moveToBrokers(Map.of(TopicPartition.of(topic, 0), ids)).toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));

      var newPartitions = admin.partitions(Set.of(topic)).toCompletableFuture().get();
      Assertions.assertEquals(1, partitions.size());
      var newPartition = newPartitions.get(0);
      Assertions.assertEquals(ids.size(), newPartition.replicas().size());
      Assertions.assertEquals(ids.size(), newPartition.isr().size());
      Assertions.assertNotEquals(ids.get(0), newPartition.leader().get().id());

      admin
          .preferredLeaderElection(
              Set.of(TopicPartition.of(partition.topic(), partition.partition())))
          .toCompletableFuture()
          .get();
      Assertions.assertEquals(
          ids.get(0),
          admin.partitions(Set.of(topic)).toCompletableFuture().get().get(0).leader().get().id());
    }
  }

  @Test
  void testMoveToAnotherFolder() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));
      var replicas = admin.replicas(Set.of(topic)).toCompletableFuture().get();
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
          .get();
      Utils.sleep(Duration.ofSeconds(2));

      var newReplicas = admin.replicas(Set.of(topic)).toCompletableFuture().get();
      Assertions.assertEquals(1, newReplicas.size());

      var newReplica = newReplicas.get(0);
      Assertions.assertEquals(idAndFolder.get(newReplica.nodeInfo().id()), newReplica.path());
    }
  }

  @Test
  void testSetAndUnsetTopicConfig() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));

      admin
          .setConfigs(topic, Map.of(TopicConfigs.FILE_DELETE_DELAY_MS_CONFIG, "3000"))
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));
      var config = admin.topics(Set.of(topic)).toCompletableFuture().get().get(0).config();
      Assertions.assertEquals("3000", config.value(TopicConfigs.FILE_DELETE_DELAY_MS_CONFIG).get());

      admin
          .unsetConfigs(topic, Set.of(TopicConfigs.FILE_DELETE_DELAY_MS_CONFIG))
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));
      config = admin.topics(Set.of(topic)).toCompletableFuture().get().get(0).config();
      Assertions.assertNotEquals(
          "3000", config.value(TopicConfigs.FILE_DELETE_DELAY_MS_CONFIG).get());
    }
  }

  @Test
  void testSetAndUnsetBrokerConfig() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var broker = admin.brokers().toCompletableFuture().get().get(0);
      var id = broker.id();
      Assertions.assertEquals("producer", broker.config().value("compression.type").get());

      admin
          .setConfigs(id, Map.of(BrokerConfigs.COMPRESSION_TYPE_CONFIG, "gzip"))
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));
      broker =
          admin.brokers().toCompletableFuture().get().stream()
              .filter(b -> b.id() == id)
              .findFirst()
              .get();
      Assertions.assertEquals(
          "gzip", broker.config().value(BrokerConfigs.COMPRESSION_TYPE_CONFIG).get());

      admin
          .unsetConfigs(id, Set.of(BrokerConfigs.COMPRESSION_TYPE_CONFIG))
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));
      broker =
          admin.brokers().toCompletableFuture().get().stream()
              .filter(b -> b.id() == id)
              .findFirst()
              .get();
      Assertions.assertNotEquals(
          "gzip", broker.config().value(BrokerConfigs.COMPRESSION_TYPE_CONFIG).get());
    }
  }

  @Test
  void testMigrateToOtherBrokers() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().get();

      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(1, admin.replicas(Set.of(topic)).toCompletableFuture().get().size());
      admin
          .moveToBrokers(Map.of(TopicPartition.of(topic, 0), new ArrayList<>(brokerIds())))
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(3, admin.replicas(Set.of(topic)).toCompletableFuture().get().size());

      admin
          .moveToBrokers(
              Map.of(TopicPartition.of(topic, 0), List.of(brokerIds().iterator().next())))
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(1, admin.replicas(Set.of(topic)).toCompletableFuture().get().size());
    }
  }

  @Test
  void testMigrateToOtherFolders() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(1, admin.replicas(Set.of(topic)).toCompletableFuture().get().size());

      var id =
          admin
              .replicas(Set.of(topic))
              .toCompletableFuture()
              .get()
              .iterator()
              .next()
              .nodeInfo()
              .id();
      var paths = new ArrayList<>(logFolders().get(id));

      for (var path : paths) {
        admin
            .moveToFolders(Map.of(TopicPartitionReplica.of(topic, 0, id), path))
            .toCompletableFuture()
            .get();
        Utils.sleep(Duration.ofSeconds(2));
        Assertions.assertEquals(
            path,
            admin.replicas(Set.of(topic)).toCompletableFuture().get().iterator().next().path());
      }
    }
  }

  @Test
  void testCreator() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .configs(Map.of(TopicConfigs.COMPRESSION_TYPE_CONFIG, "lz4"))
          .run()
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));

      var config = admin.topics(Set.of(topic)).toCompletableFuture().get().get(0).config();
      config.raw().keySet().forEach(key -> Assertions.assertTrue(config.value(key).isPresent()));
      Assertions.assertTrue(config.raw().containsValue("lz4"));
    }
  }

  @Test
  void testCreateTopicRepeatedly() {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      Runnable createTopic =
          () ->
              Utils.packException(
                  () ->
                      admin
                          .creator()
                          .configs(Map.of(TopicConfigs.COMPRESSION_TYPE_CONFIG, "lz4"))
                          .numberOfReplicas((short) 1)
                          .numberOfPartitions(3)
                          .topic(topic)
                          .run()
                          .toCompletableFuture()
                          .get());

      createTopic.run();
      Utils.waitFor(
          () ->
              Utils.packException(
                  () -> admin.topicNames(true).toCompletableFuture().get().contains(topic)));
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
                          .topic(topic)
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
                          .topic(topic)
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
                          .topic(topic)
                          .run()
                          .toCompletableFuture()
                          .get())
              .getCause());
    }
  }

  @Test
  void testPartitions() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
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

      admin.creator().topic(topic).numberOfPartitions(10).run().toCompletableFuture().get();
      // wait for syncing topic creation
      Utils.sleep(Duration.ofSeconds(5));
      Assertions.assertTrue(admin.topicNames(true).toCompletableFuture().get().contains(topic));
      var partitions = admin.replicas(Set.of(topic)).toCompletableFuture().get();
      Assertions.assertEquals(10, partitions.size());
      var logFolders =
          logFolders().values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
      partitions.forEach(
          replica -> Assertions.assertTrue(logFolders.stream().anyMatch(replica.path()::contains)));
      brokerIds()
          .forEach(
              id -> {
                try {
                  Assertions.assertNotEquals(
                      0,
                      admin.topicPartitionReplicas(Set.of(id)).toCompletableFuture().get().size());
                } catch (InterruptedException | ExecutionException e) {
                  throw new RuntimeException(e);
                }
              });

      var after =
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

      Assertions.assertEquals(before + 10, after);
    }
  }

  @Test
  void testOffsets() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(3).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));
      var partitions = admin.partitions(Set.of(topic)).toCompletableFuture().get();
      Assertions.assertEquals(3, partitions.size());
      partitions.forEach(
          p -> {
            Assertions.assertEquals(0, p.earliestOffset());
            Assertions.assertEquals(0, p.latestOffset());
          });
      try (var producer = Producer.of(bootstrapServers())) {
        producer.sender().topic(topic).key(new byte[100]).partition(0).run();
        producer.sender().topic(topic).key(new byte[55]).partition(1).run();
        producer.sender().topic(topic).key(new byte[33]).partition(2).run();
      }

      admin
          .partitions(Set.of(topic))
          .toCompletableFuture()
          .get()
          .forEach(p -> Assertions.assertEquals(0, p.earliestOffset()));
      admin
          .partitions(Set.of(topic))
          .toCompletableFuture()
          .get()
          .forEach(p -> Assertions.assertEquals(1, p.latestOffset()));
      admin
          .partitions(Set.of(topic))
          .toCompletableFuture()
          .get()
          .forEach(p -> Assertions.assertNotEquals(Optional.empty(), p.maxTimestamp()));
    }
  }

  @Test
  void testConsumerGroups() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    var consumerGroup = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(3).run().toCompletableFuture().get();
      try (var c1 =
          Consumer.forTopics(Set.of(topic))
              .bootstrapServers(bootstrapServers())
              .config(ConsumerConfigs.GROUP_ID_CONFIG, consumerGroup)
              .build()) {
        // wait for syncing topic creation
        Utils.sleep(Duration.ofSeconds(5));
        var consumerGroupMap =
            admin.consumerGroups(Set.of(consumerGroup)).toCompletableFuture().get();
        Assertions.assertEquals(1, consumerGroupMap.size());
        Assertions.assertTrue(
            consumerGroupMap.stream().anyMatch(cg -> cg.groupId().equals(consumerGroup)));

        try (var c2 =
            Consumer.forTopics(Set.of(topic))
                .bootstrapServers(bootstrapServers())
                .config(ConsumerConfigs.GROUP_ID_CONFIG, "abc")
                .build()) {
          var count =
              admin.consumerGroupIds().toCompletableFuture().get().stream()
                  .mapToInt(
                      t ->
                          Utils.packException(
                              () ->
                                  admin
                                      .consumerGroups(Set.of(t))
                                      .toCompletableFuture()
                                      .get()
                                      .size()))
                  .sum();
          Assertions.assertEquals(
              count,
              admin
                  .consumerGroups(admin.consumerGroupIds().toCompletableFuture().get())
                  .toCompletableFuture()
                  .get()
                  .size());
          Assertions.assertEquals(
              1, admin.consumerGroups(Set.of("abc")).toCompletableFuture().get().size());
        }
      }
    }
  }

  @Test
  void testMigrateSinglePartition() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().get();

      Utils.sleep(Duration.ofSeconds(5));
      var broker = brokerIds().iterator().next();
      admin.moveToBrokers(Map.of(TopicPartition.of(topic, 0), List.of(broker)));

      Utils.waitFor(
          () ->
              Utils.packException(
                  () -> {
                    var partitionReplicas =
                        admin.replicas(Set.of(topic)).toCompletableFuture().get();
                    return partitionReplicas.size() == 1
                        && partitionReplicas.get(0).nodeInfo().id() == broker;
                  }));

      var currentBroker =
          admin.replicas(Set.of(topic)).toCompletableFuture().get().stream()
              .filter(replica -> replica.partition() == 0)
              .findFirst()
              .get()
              .nodeInfo()
              .id();
      var allPath = admin.brokerFolders().toCompletableFuture().get();
      var otherPath =
          allPath.get(currentBroker).stream()
              .filter(
                  i ->
                      Utils.packException(
                          () ->
                              !i.contains(
                                  admin.replicas(Set.of(topic)).toCompletableFuture().get().stream()
                                      .filter(replica -> replica.partition() == 0)
                                      .findFirst()
                                      .get()
                                      .path())))
              .collect(Collectors.toSet());

      admin.moveToFolders(
          Map.of(TopicPartitionReplica.of(topic, 0, currentBroker), otherPath.iterator().next()));
      Utils.waitFor(
          () ->
              Utils.packException(
                  () -> {
                    var partitionReplicas =
                        admin.replicas(Set.of(topic)).toCompletableFuture().get();
                    return partitionReplicas.size() == 1
                        && partitionReplicas.get(0).path().equals(otherPath.iterator().next());
                  }));
    }
  }

  @Test
  void testIllegalMigrationArgument() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    var topicParition = TopicPartition.of(topic, 0);
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(1));
      var currentReplica =
          admin.replicas(Set.of(topic)).toCompletableFuture().get().stream()
              .filter(replica -> replica.partition() == topicParition.partition())
              .findFirst()
              .get();

      var currentBroker = currentReplica.nodeInfo().id();
      var notExistReplica = (currentBroker + 1) % brokerIds().size();
      var nextDir = logFolders().get(notExistReplica).iterator().next();

      Assertions.assertThrows(
          ExecutionException.class,
          () ->
              admin
                  .moveToFolders(Map.of(TopicPartitionReplica.of(topic, 0, currentBroker), nextDir))
                  .toCompletableFuture()
                  .get());
    }
  }

  @Test
  void testMigrateAllPartitions() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(3).run().toCompletableFuture().get();

      Utils.sleep(Duration.ofSeconds(5));
      var broker = brokerIds().iterator().next();
      admin
          .partitions(Set.of(topic))
          .toCompletableFuture()
          .get()
          .forEach(p -> admin.moveToBrokers(Map.of(p.topicPartition(), List.of(broker))));
      Utils.waitFor(
          () ->
              Utils.packException(
                  () -> {
                    var replicas = admin.replicas(Set.of(topic)).toCompletableFuture().get();
                    return replicas.stream().allMatch(r -> r.nodeInfo().id() == broker);
                  }));
    }
  }

  @Test
  void testReplicaSize() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers());
        var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      producer.sender().topic(topic).key(new byte[100]).run().toCompletableFuture().get();
      var originSize =
          admin
              .replicas(Set.of(topic))
              .thenApply(
                  replicas ->
                      replicas.stream()
                          .filter(replica -> replica.partition() == 0)
                          .findFirst()
                          .get()
                          .size())
              .toCompletableFuture()
              .get();

      // add data again
      producer.sender().topic(topic).key(new byte[100]).run().toCompletableFuture().get();

      var newSize =
          admin
              .replicas(Set.of(topic))
              .thenApply(
                  replicas ->
                      replicas.stream()
                          .filter(replica -> replica.partition() == 0)
                          .findFirst()
                          .get()
                          .size())
              .toCompletableFuture()
              .get();
      Assertions.assertTrue(newSize > originSize);
    }
  }

  @Test
  void testCompact() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
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
          .get();

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
            .forEach(i -> producer.sender().key(key).value(value).topic(topic).run());
        producer.flush();

        Utils.sleep(Duration.ofSeconds(2));
        IntStream.range(0, 10)
            .forEach(i -> producer.sender().key(anotherKey).value(value).topic(topic).run());
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
  void testBrokers() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(6)
          .run()
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));
      var brokers = admin.brokers().toCompletableFuture().get();
      Assertions.assertEquals(3, brokers.size());
      brokers.forEach(broker -> Assertions.assertNotEquals(0, broker.config().raw().size()));
      Assertions.assertEquals(1, brokers.stream().filter(Broker::isController).count());
      brokers.forEach(broker -> Assertions.assertNotEquals(0, broker.topicPartitions().size()));
      brokers.forEach(
          broker -> Assertions.assertNotEquals(0, broker.topicPartitionLeaders().size()));
    }
  }

  @Test
  void testBrokerFolders() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      Assertions.assertEquals(
          brokerIds().size(), admin.brokers().toCompletableFuture().get().size());
      // list all
      logFolders()
          .forEach(
              (id, ds) ->
                  Utils.packException(
                      () ->
                          Assertions.assertEquals(
                              admin.brokerFolders().toCompletableFuture().get().get(id).size(),
                              ds.size())));

      admin
          .brokers()
          .toCompletableFuture()
          .get()
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
  void testReplicas() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(2).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          2,
          admin.replicas(Set.of(topic)).toCompletableFuture().get().stream()
              .collect(
                  Collectors.groupingBy(
                      replica -> TopicPartition.of(replica.topic(), replica.partition())))
              .size());

      var count =
          admin
              .replicas(admin.topicNames(true).toCompletableFuture().get())
              .toCompletableFuture()
              .get()
              .stream()
              .collect(
                  Collectors.groupingBy(
                      replica -> TopicPartition.of(replica.topic(), replica.partition())))
              .size();
      Assertions.assertEquals(
          count,
          admin
              .replicas(admin.topicNames(true).toCompletableFuture().get())
              .toCompletableFuture()
              .get()
              .stream()
              .collect(
                  Collectors.groupingBy(
                      replica -> TopicPartition.of(replica.topic(), replica.partition())))
              .size());
    }
  }

  @Test
  void testReplicasPreferredLeaderFlag() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    var partitionCount = 10;
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitionCount)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .get();

      Utils.sleep(Duration.ofSeconds(3));

      var expectedPreferredLeader =
          IntStream.range(0, partitionCount)
              .mapToObj(p -> TopicPartition.of(topic, p))
              .collect(Collectors.toUnmodifiableMap(p -> p, p -> List.of(0)));

      var currentPreferredLeader =
          (Supplier<Map<TopicPartition, List<Integer>>>)
              () ->
                  Utils.packException(
                      () ->
                          admin.replicas(Set.of(topic)).toCompletableFuture().get().stream()
                              .filter(Replica::isPreferredLeader)
                              .collect(
                                  Collectors.groupingBy(
                                      replica ->
                                          TopicPartition.of(replica.topic(), replica.partition()),
                                      Collectors.mapping(
                                          replica -> replica.nodeInfo().id(),
                                          Collectors.toList()))));

      IntStream.range(0, partitionCount)
          .forEach(p -> admin.moveToBrokers(Map.of(TopicPartition.of(topic, p), List.of(0, 1, 2))));
      Utils.sleep(Duration.ofSeconds(3));

      Assertions.assertEquals(expectedPreferredLeader, currentPreferredLeader.get());
    }
  }

  @Test
  void testProducerStates() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var producer = Producer.of(bootstrapServers());
        var admin = AsyncAdmin.of(bootstrapServers())) {
      producer.sender().topic(topic).value(new byte[1]).run().toCompletableFuture().get();

      var states =
          admin
              .producerStates(admin.topicPartitions(Set.of(topic)).toCompletableFuture().get())
              .toCompletableFuture()
              .get();
      Assertions.assertNotEquals(0, states.size());
      var producerState =
          states.stream()
              .filter(s -> s.topic().equals(topic))
              .collect(Collectors.toUnmodifiableList());
      Assertions.assertEquals(1, producerState.size());
    }
  }

  @Test
  void testConnectionQuotas() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.setConnectionQuotas(Map.of(Utils.hostname(), 100)).toCompletableFuture().get();

      var quotas =
          admin.quotas(Set.of(QuotaConfigs.IP)).toCompletableFuture().get().stream()
              .filter(q -> q.targetValue().equals(Utils.hostname()))
              .collect(Collectors.toList());
      Assertions.assertNotEquals(0, quotas.size());
      quotas.forEach(
          quota -> {
            Assertions.assertEquals(Utils.hostname(), quota.targetValue());
            Assertions.assertEquals(QuotaConfigs.IP_CONNECTION_RATE_CONFIG, quota.limitKey());
            Assertions.assertEquals(100D, quota.limitValue());
          });

      admin.unsetConnectionQuotas(Set.of(Utils.hostname())).toCompletableFuture().get();
      Assertions.assertEquals(
          0,
          (int)
              admin.quotas().toCompletableFuture().get().stream()
                  .filter(q -> q.targetKey().equals(QuotaConfigs.IP))
                  .filter(q -> q.targetValue().equals(Utils.hostname()))
                  .filter(q -> q.limitKey().equals(QuotaConfigs.IP_CONNECTION_RATE_CONFIG))
                  .count());
    }
  }

  @Test
  void testProducerQuotas() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin
          .setProducerQuotas(Map.of(Utils.hostname(), DataRate.Byte.of(100).perSecond()))
          .toCompletableFuture()
          .get();

      var quotas =
          admin.quotas(Set.of(QuotaConfigs.CLIENT_ID)).toCompletableFuture().get().stream()
              .filter(q -> q.targetValue().equals(Utils.hostname()))
              .filter(q -> q.limitKey().equals(QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG))
              .collect(Collectors.toList());
      Assertions.assertNotEquals(0, quotas.size());
      quotas.forEach(
          quota ->
              Assertions.assertEquals(
                  DataRate.Byte.of(100).perSecond().byteRate(), quota.limitValue()));

      admin.unsetProducerQuotas(Set.of(Utils.hostname())).toCompletableFuture().get();
      Assertions.assertEquals(
          0,
          (int)
              admin.quotas().toCompletableFuture().get().stream()
                  .filter(q -> q.targetKey().equals(QuotaConfigs.CLIENT_ID))
                  .filter(q -> q.targetValue().equals(Utils.hostname()))
                  .filter(q -> q.limitKey().equals(QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG))
                  .count());
    }
  }

  @Test
  void testConsumerQuotas() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin
          .setConsumerQuotas(Map.of(Utils.hostname(), DataRate.Byte.of(1000).perSecond()))
          .toCompletableFuture()
          .get();

      var quotas =
          admin.quotas(Set.of(QuotaConfigs.CLIENT_ID)).toCompletableFuture().get().stream()
              .filter(q -> q.targetValue().equals(Utils.hostname()))
              .filter(q -> q.limitKey().equals(QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG))
              .collect(Collectors.toList());
      Assertions.assertNotEquals(0, quotas.size());
      quotas.forEach(
          quota ->
              Assertions.assertEquals(
                  DataRate.Byte.of(1000).perSecond().byteRate(), quota.limitValue()));

      admin.unsetConsumerQuotas(Set.of(Utils.hostname())).toCompletableFuture().get();
      Assertions.assertEquals(
          0,
          (int)
              admin.quotas().toCompletableFuture().get().stream()
                  .filter(q -> q.targetKey().equals(QuotaConfigs.CLIENT_ID))
                  .filter(q -> q.targetValue().equals(Utils.hostname()))
                  .filter(q -> q.limitKey().equals(QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG))
                  .count());
    }
  }

  @Test
  void testSizeOfNoDataTopic() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(3));

      admin
          .brokers()
          .toCompletableFuture()
          .get()
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
  void testDeleteRecord() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));
      var deleteRecords =
          admin.deleteRecords(Map.of(TopicPartition.of(topic, 0), 0L)).toCompletableFuture().get();

      Assertions.assertEquals(1, deleteRecords.size());
      Assertions.assertEquals(0, deleteRecords.values().stream().findFirst().get());

      try (var producer = Producer.of(bootstrapServers())) {
        var senders =
            Stream.of(0, 0, 0, 1, 1)
                .map(x -> producer.sender().topic(topic).partition(x).value(new byte[100]))
                .collect(Collectors.toList());
        producer.send(senders);
        producer.flush();
      }

      deleteRecords =
          admin
              .deleteRecords(
                  Map.of(TopicPartition.of(topic, 0), 2L, TopicPartition.of(topic, 1), 1L))
              .toCompletableFuture()
              .get();
      Assertions.assertEquals(2, deleteRecords.size());
      Assertions.assertEquals(2, deleteRecords.get(TopicPartition.of(topic, 0)));
      Assertions.assertEquals(1, deleteRecords.get(TopicPartition.of(topic, 1)));

      var partitions = admin.partitions(Set.of(topic)).toCompletableFuture().get();
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
  void testDeleteTopic() throws ExecutionException, InterruptedException {
    var topic =
        IntStream.range(0, 4).mapToObj(x -> Utils.randomString()).collect(Collectors.toList());

    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      topic.forEach(
          x ->
              Utils.packException(
                  () ->
                      admin
                          .creator()
                          .topic(x)
                          .numberOfPartitions(3)
                          .numberOfReplicas((short) 3)
                          .run()
                          .toCompletableFuture()
                          .get()));
      Utils.sleep(Duration.ofSeconds(2));

      admin.deleteTopics(Set.of(topic.get(0), topic.get(1)));
      Utils.sleep(Duration.ofSeconds(2));

      var latestTopicNames = admin.topicNames(true).toCompletableFuture().get();
      Assertions.assertFalse(latestTopicNames.contains(topic.get(0)));
      Assertions.assertFalse(latestTopicNames.contains(topic.get(1)));
      Assertions.assertTrue(latestTopicNames.contains(topic.get(2)));
      Assertions.assertTrue(latestTopicNames.contains(topic.get(3)));

      admin.deleteTopics(Set.of(topic.get(3)));
      Utils.sleep(Duration.ofSeconds(2));

      latestTopicNames = admin.topicNames(true).toCompletableFuture().get();
      Assertions.assertFalse(latestTopicNames.contains(topic.get(3)));
      Assertions.assertTrue(latestTopicNames.contains(topic.get(2)));
    }
  }

  @Test
  void testListPartitions() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(2)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));
      var partitions = admin.partitions(Set.of(topic)).toCompletableFuture().get();
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
  void testRunningRequests() throws ExecutionException, InterruptedException {
    try (AsyncAdminImpl admin = (AsyncAdminImpl) AsyncAdmin.of(bootstrapServers())) {
      Assertions.assertEquals(0, admin.runningRequests());
      var f0 = new KafkaFutureImpl<Integer>();
      var f1 = admin.to(f0);
      Assertions.assertEquals(1, admin.runningRequests());
      f0.complete(10);
      Assertions.assertEquals(0, admin.runningRequests());
      Assertions.assertEquals(10, f1.toCompletableFuture().get());
    }
  }

  @Test
  void testDeleteMembers() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      try (var producer = Producer.of(bootstrapServers())) {
        producer.sender().topic(topic).key(new byte[10]).run().toCompletableFuture().get();
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
        admin.deleteMembers(Set.of(consumer.groupId())).toCompletableFuture().get();
        groupId = consumer.groupId();
      }
      admin.deleteMembers(Set.of(groupId)).toCompletableFuture().get();

      var gs = admin.consumerGroups(Set.of(groupId)).toCompletableFuture().get();
      Assertions.assertEquals(1, gs.size());
      Assertions.assertEquals(0, gs.get(0).assignment().size());
    }
  }

  @Test
  void testDeleteGroups() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      try (var producer = Producer.of(bootstrapServers())) {
        producer.sender().topic(topic).key(new byte[10]).run().toCompletableFuture().get();
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
      admin.deleteGroups(Set.of(groupId)).toCompletableFuture().get();
      Assertions.assertFalse(
          admin.consumerGroupIds().toCompletableFuture().get().contains(groupId));
    }
  }

  @Test
  void testRemoveStaticMembers() throws ExecutionException, InterruptedException {
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
      try (var admin = AsyncAdmin.of(bootstrapServers())) {
        Assertions.assertEquals(
            1,
            admin
                .consumerGroups(Set.of(groupId))
                .toCompletableFuture()
                .get()
                .get(0)
                .assignment()
                .keySet()
                .stream()
                .filter(m -> m.groupInstanceId().map(id -> id.equals(staticMember)).isPresent())
                .count());
        admin
            .deleteInstanceMembers(Map.of(groupId, Set.of(staticMember)))
            .toCompletableFuture()
            .get();
        Assertions.assertEquals(
            0,
            admin
                .consumerGroups(Set.of(groupId))
                .toCompletableFuture()
                .get()
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
  void testQueryNonexistentGroup() throws ExecutionException, InterruptedException {
    var group = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var groups = admin.consumerGroups(Set.of(group)).toCompletableFuture().get();
      Assertions.assertEquals(1, groups.size());
      Assertions.assertEquals(0, groups.get(0).assignment().size());
      Assertions.assertEquals(0, groups.get(0).consumeProgress().size());
    }
  }

  @Test
  void testPreferredLeaders() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));
      admin
          .preferredLeaderElection(Set.of(TopicPartition.of(topic, 0)))
          .toCompletableFuture()
          .get();
    }
  }

  @Test
  public void testTimestampOfLatestRecords() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(4).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(3));
      var ps = admin.topicPartitions(Set.of(topic)).toCompletableFuture().get();
      Assertions.assertEquals(
          0,
          admin
              .timestampOfLatestRecords(ps, Duration.ofSeconds(3))
              .toCompletableFuture()
              .get()
              .size());

      var t = System.currentTimeMillis();
      try (var producer = Producer.of(bootstrapServers())) {
        producer
            .sender()
            .topic(topic)
            .partition(0)
            .timestamp(t)
            .key(Utils.randomString().getBytes(StandardCharsets.UTF_8))
            .run();
        producer
            .sender()
            .topic(topic)
            .partition(1)
            .timestamp(t + 1)
            .key(Utils.randomString().getBytes(StandardCharsets.UTF_8))
            .run();
        producer
            .sender()
            .topic(topic)
            .partition(2)
            .timestamp(t + 2)
            .key(Utils.randomString().getBytes(StandardCharsets.UTF_8))
            .run();
        producer
            .sender()
            .topic(topic)
            .partition(3)
            .timestamp(t + 3)
            .key(Utils.randomString().getBytes(StandardCharsets.UTF_8))
            .run();
        producer.flush();
      }
      var ts =
          admin.timestampOfLatestRecords(ps, Duration.ofSeconds(3)).toCompletableFuture().get();
      Assertions.assertEquals(4, ts.size());
      Assertions.assertEquals(t, ts.get(TopicPartition.of(topic, 0)));
      Assertions.assertEquals(t + 1, ts.get(TopicPartition.of(topic, 1)));
      Assertions.assertEquals(t + 2, ts.get(TopicPartition.of(topic, 2)));
      Assertions.assertEquals(t + 3, ts.get(TopicPartition.of(topic, 3)));
    }
  }
}
