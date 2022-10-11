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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AsyncAdminTest extends RequireBrokerCluster {

  @Test
  void testMoveToAnotherBroker() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));

      var partitions = admin.partitions(Set.of(topic)).toCompletableFuture().get();
      Assertions.assertEquals(1, partitions.size());
      var partition = partitions.get(0);
      Assertions.assertEquals(1, partition.replicas().size());

      var ids =
          brokerIds().stream()
              .filter(i -> i != partition.leader().get().id())
              .collect(Collectors.toList());
      admin.migrator().topic(topic).moveTo(ids).toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));

      var newPartitions = admin.partitions(Set.of(topic)).toCompletableFuture().get();
      Assertions.assertEquals(1, partitions.size());
      var newPartition = newPartitions.get(0);
      Assertions.assertEquals(ids.size(), newPartition.replicas().size());
      Assertions.assertEquals(ids.size(), newPartition.isr().size());
      Assertions.assertEquals(ids.get(0), newPartition.leader().get().id());
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
      admin.migrator().topic(topic).moveTo(ids).toCompletableFuture().get();
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

      admin.migrator().topic(topic).moveTo(idAndFolder).toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));

      var newReplicas = admin.replicas(Set.of(topic)).toCompletableFuture().get();
      Assertions.assertEquals(1, newReplicas.size());

      var newReplica = newReplicas.get(0);
      Assertions.assertEquals(idAndFolder.get(newReplica.nodeInfo().id()), newReplica.dataFolder());
    }
  }

  @Test
  void testMoveToNotHostedFolder() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));
      var replicas = admin.replicas(Set.of(topic)).toCompletableFuture().get();
      Assertions.assertEquals(1, replicas.size());

      var replica = replicas.get(0);
      var idAndFolder =
          logFolders().entrySet().stream()
              .filter(e -> e.getKey() != replica.nodeInfo().id())
              .map(e -> Map.of(e.getKey(), e.getValue().iterator().next()))
              .findFirst()
              .get();
      Assertions.assertEquals(1, idAndFolder.size());

      Assertions.assertInstanceOf(
          IllegalStateException.class,
          Assertions.assertThrows(
                  ExecutionException.class,
                  () ->
                      admin.migrator().topic(topic).moveTo(idAndFolder).toCompletableFuture().get())
              .getCause());
    }
  }

  @Test
  void testUpdateTopicConfig() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));

      var config = admin.topics(Set.of(topic)).toCompletableFuture().get().get(0).config();
      Assertions.assertEquals("delete", config.value("cleanup.policy").get());

      admin.updateConfig(topic, Map.of("cleanup.policy", "compact")).toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));
      config = admin.topics(Set.of(topic)).toCompletableFuture().get().get(0).config();
      Assertions.assertEquals("compact", config.value("cleanup.policy").get());
    }
  }

  @Test
  void testUpdateBrokerConfig() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString();
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var broker = admin.brokers().toCompletableFuture().get().get(0);
      Assertions.assertEquals("producer", broker.config().value("compression.type").get());

      admin
          .updateConfig(broker.id(), Map.of("compression.type", "gzip"))
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));
      var broker2 =
          admin.brokers().toCompletableFuture().get().stream()
              .filter(b -> b.id() == broker.id())
              .findFirst()
              .get();
      Assertions.assertEquals("gzip", broker2.config().value("compression.type").get());
    }
  }
}
