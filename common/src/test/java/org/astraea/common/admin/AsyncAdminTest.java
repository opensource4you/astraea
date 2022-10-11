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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AsyncAdminTest extends RequireBrokerCluster {

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
          .preferredLeaderElection(TopicPartition.of(partition.topic(), partition.partition()))
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
      Assertions.assertEquals(idAndFolder.get(newReplica.nodeInfo().id()), newReplica.dataFolder());
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
            admin
                .replicas(Set.of(topic))
                .toCompletableFuture()
                .get()
                .iterator()
                .next()
                .dataFolder());
      }
    }
  }

  @Test
  void testCreator() throws ExecutionException, InterruptedException {
    var topic = "testCreator";
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
  void testPartitions() throws ExecutionException, InterruptedException {
    var topic = "testPartitions";
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var before =
          brokerIds().stream()
              .mapToInt(
                  id -> {
                    try {
                      return admin
                          .topicPartitionReplicas(Set.of(id))
                          .toCompletableFuture()
                          .get()
                          .size();
                    } catch (InterruptedException | ExecutionException e) {
                      throw new RuntimeException(e);
                    }
                  })
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
          replica ->
              Assertions.assertTrue(logFolders.stream().anyMatch(replica.dataFolder()::contains)));
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
                  id -> {
                    try {
                      return admin
                          .topicPartitionReplicas(Set.of(id))
                          .toCompletableFuture()
                          .get()
                          .size();
                    } catch (InterruptedException | ExecutionException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .sum();

      Assertions.assertEquals(before + 10, after);
    }
  }

  @Test
  void testConsumerGroups() throws ExecutionException, InterruptedException {
    var topic = "testConsumerGroups-Topic";
    var consumerGroup = "testConsumerGroups-Group";
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
                      t -> {
                        try {
                          return admin.consumerGroups(Set.of(t)).toCompletableFuture().get().size();
                        } catch (InterruptedException | ExecutionException e) {
                          throw new RuntimeException(e);
                        }
                      })
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
}
