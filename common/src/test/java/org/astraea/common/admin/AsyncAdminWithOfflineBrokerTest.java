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
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class AsyncAdminWithOfflineBrokerTest extends RequireBrokerCluster {

  private static final String TOPIC_NAME = Utils.randomString();
  private static final int CLOSED_BROKER_ID = brokerIds().iterator().next();

  @BeforeAll
  static void closeOneBroker() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(TOPIC_NAME).numberOfPartitions(10).run().toCompletableFuture().get();
      var allPs = admin.partitions(Set.of(TOPIC_NAME)).toCompletableFuture().get();
      Assertions.assertEquals(10, allPs.size());
      Utils.sleep(Duration.ofSeconds(2));
    }
    closeBroker(CLOSED_BROKER_ID);
  }

  @Timeout(10)
  @Test
  void testNodeInfos() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var nodeInfos = admin.nodeInfos().toCompletableFuture().get();
      Assertions.assertEquals(2, nodeInfos.size());
      var offlineNodeInfos =
          nodeInfos.stream().filter(NodeInfo::offline).collect(Collectors.toList());
      Assertions.assertEquals(0, offlineNodeInfos.size());
    }
  }

  @Timeout(10)
  @Test
  void testBrokers() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var brokers = admin.brokers().toCompletableFuture().get();
      Assertions.assertEquals(2, brokers.size());
      brokers.forEach(
          b ->
              b.folders()
                  .forEach(d -> Assertions.assertEquals(0, d.orphanPartitionSizes().size())));
      var offlineBrokers = brokers.stream().filter(NodeInfo::offline).collect(Collectors.toList());
      Assertions.assertEquals(0, offlineBrokers.size());
    }
  }

  @Timeout(10)
  @Test
  void testPartitions() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var partitions = admin.partitions(Set.of(TOPIC_NAME)).toCompletableFuture().get();
      Assertions.assertEquals(10, partitions.size());
      var offlinePartitions =
          partitions.stream().filter(p -> p.leader().isEmpty()).collect(Collectors.toList());
      offlinePartitions.forEach(
          p -> {
            Assertions.assertEquals(1, p.replicas().size());
            p.replicas().forEach(n -> Assertions.assertTrue(n.offline()));
            // there is no more data, so all replicas are in-sync
            Assertions.assertEquals(1, p.isr().size());
            p.isr().forEach(n -> Assertions.assertTrue(n.offline()));
          });
      Assertions.assertNotEquals(0, offlinePartitions.size());
    }
  }

  @Timeout(10)
  @Test
  void testReplicas() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var replicas = admin.replicas(Set.of(TOPIC_NAME)).toCompletableFuture().get();
      Assertions.assertEquals(10, replicas.size());
      var offlineReplicas =
          replicas.stream().filter(ReplicaInfo::isOffline).collect(Collectors.toList());
      offlineReplicas.forEach(r -> Assertions.assertTrue(r.nodeInfo().offline()));
      Assertions.assertNotEquals(0, offlineReplicas.size());
    }
  }

  @Timeout(10)
  @Test
  void testTopicPartitions() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      Assertions.assertNotEquals(
          0,
          admin
              .topicPartitionReplicas(Set.of(CLOSED_BROKER_ID))
              .toCompletableFuture()
              .get()
              .size());
      Assertions.assertNotEquals(
          0, admin.topicPartitions(Set.of(TOPIC_NAME)).toCompletableFuture().get().size());
    }
  }

  @Timeout(10)
  @Test
  void testTopicNames() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      Assertions.assertEquals(1, admin.topicNames(false).toCompletableFuture().get().size());
    }
  }

  @Timeout(10)
  @Test
  void testTopics() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      Assertions.assertEquals(
          1, admin.topics(Set.of(TOPIC_NAME)).toCompletableFuture().get().size());
    }
  }
}
