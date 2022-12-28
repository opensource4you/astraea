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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class AdminWithOfflineBrokerTest extends RequireBrokerCluster {

  private static final String TOPIC_NAME = Utils.randomString();
  private static final int PARTITIONS = 10;

  private static final Set<TopicPartition> TOPIC_PARTITIONS =
      IntStream.range(0, PARTITIONS)
          .mapToObj(id -> TopicPartition.of(TOPIC_NAME, id))
          .collect(Collectors.toSet());
  private static final int CLOSED_BROKER_ID = brokerIds().iterator().next();

  private static int NUMBER_OF_ONLINE_PARTITIONS = -1;

  @BeforeAll
  static void closeOneBroker() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(TOPIC_NAME)
          .numberOfPartitions(PARTITIONS)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(3));
      try (var producer = Producer.of(bootstrapServers())) {
        IntStream.range(0, PARTITIONS)
            .forEach(
                i ->
                    producer
                        .send(
                            Record.builder()
                                .topic(TOPIC_NAME)
                                .partition(i)
                                .key(new byte[100])
                                .build())
                        .toCompletableFuture()
                        .join());
      }

      var allPs = admin.partitions(Set.of(TOPIC_NAME)).toCompletableFuture().join();
      NUMBER_OF_ONLINE_PARTITIONS =
          PARTITIONS
              - (int) allPs.stream().filter(p -> p.leader().get().id() == CLOSED_BROKER_ID).count();
      Assertions.assertEquals(PARTITIONS, allPs.size());
      Utils.sleep(Duration.ofSeconds(2));
    }
    closeBroker(CLOSED_BROKER_ID);
  }

  @Timeout(10)
  @Test
  void testProducerStates() {
    try (var admin = Admin.of(bootstrapServers())) {
      var producerStates =
          admin
              .producerStates(
                  admin
                      .topicPartitions(admin.topicNames(true).toCompletableFuture().join())
                      .toCompletableFuture()
                      .join())
              .toCompletableFuture()
              .join();
      Assertions.assertNotEquals(0, producerStates.size());
      // we can't get producer states from offline brokers
      Assertions.assertNotEquals(PARTITIONS, producerStates.size());
    }
  }

  @Timeout(10)
  @Test
  void testNodeInfos() {
    try (var admin = Admin.of(bootstrapServers())) {
      var nodeInfos = admin.nodeInfos().toCompletableFuture().join();
      Assertions.assertEquals(2, nodeInfos.size());
      var offlineNodeInfos =
          nodeInfos.stream().filter(NodeInfo::offline).collect(Collectors.toList());
      Assertions.assertEquals(0, offlineNodeInfos.size());
    }
  }

  @Timeout(10)
  @Test
  void testBrokers() {
    try (var admin = Admin.of(bootstrapServers())) {
      var brokers = admin.brokers().toCompletableFuture().join();
      Assertions.assertEquals(2, brokers.size());
      brokers.forEach(
          b ->
              b.dataFolders()
                  .forEach(d -> Assertions.assertEquals(0, d.orphanPartitionSizes().size())));
      var offlineBrokers = brokers.stream().filter(NodeInfo::offline).collect(Collectors.toList());
      Assertions.assertEquals(0, offlineBrokers.size());
    }
  }

  @Timeout(10)
  @Test
  void testPartitions() {
    try (var admin = Admin.of(bootstrapServers())) {
      var partitions = admin.partitions(Set.of(TOPIC_NAME)).toCompletableFuture().join();
      Assertions.assertEquals(PARTITIONS, partitions.size());
      var offlinePartitions =
          partitions.stream().filter(p -> p.leader().isEmpty()).collect(Collectors.toList());
      offlinePartitions.forEach(
          p -> {
            Assertions.assertEquals(1, p.replicas().size());
            p.replicas().forEach(n -> Assertions.assertTrue(n.offline()));
            // there is no more data, so all replicas are in-sync
            Assertions.assertEquals(1, p.isr().size());
            p.isr().forEach(n -> Assertions.assertTrue(n.offline()));
            Assertions.assertEquals(Optional.empty(), p.maxTimestamp());
            Assertions.assertEquals(-1, p.earliestOffset());
            Assertions.assertEquals(-1, p.latestOffset());
          });
      Assertions.assertEquals(PARTITIONS - NUMBER_OF_ONLINE_PARTITIONS, offlinePartitions.size());
    }
  }

  @Timeout(10)
  @Test
  void testReplicas() {
    try (var admin = Admin.of(bootstrapServers())) {
      var replicas = admin.clusterInfo(Set.of(TOPIC_NAME)).toCompletableFuture().join().replicas();
      Assertions.assertEquals(PARTITIONS, replicas.size());
      var offlineReplicas =
          replicas.stream().filter(ReplicaInfo::isOffline).collect(Collectors.toList());
      Assertions.assertNotEquals(PARTITIONS, offlineReplicas.size());
      offlineReplicas.forEach(r -> Assertions.assertTrue(r.nodeInfo().offline()));
      offlineReplicas.forEach(r -> Assertions.assertNull(r.path()));
      offlineReplicas.forEach(r -> Assertions.assertEquals(-1, r.size()));
      offlineReplicas.forEach(r -> Assertions.assertEquals(-1, r.lag()));
      offlineReplicas.forEach(r -> Assertions.assertFalse(r.isFuture()));
      offlineReplicas.forEach(r -> Assertions.assertTrue(r.isPreferredLeader()));
      offlineReplicas.forEach(r -> Assertions.assertFalse(r.isLeader()));
      offlineReplicas.forEach(r -> Assertions.assertTrue(r.inSync()));
    }
  }

  @Timeout(10)
  @Test
  void testTopicPartitions() {
    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertNotEquals(
          0,
          admin
              .topicPartitionReplicas(Set.of(CLOSED_BROKER_ID))
              .toCompletableFuture()
              .join()
              .size());
      Assertions.assertNotEquals(
          0, admin.topicPartitions(Set.of(TOPIC_NAME)).toCompletableFuture().join().size());
    }
  }

  @Timeout(10)
  @Test
  void testTopicNames() {
    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertEquals(1, admin.topicNames(false).toCompletableFuture().join().size());
    }
  }

  @Timeout(10)
  @Test
  void testTopics() {
    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertEquals(
          1, admin.topics(Set.of(TOPIC_NAME)).toCompletableFuture().join().size());
    }
  }

  @Timeout(10)
  @Test
  void testEarliest() {
    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertEquals(
          0, admin.earliestOffsets(TOPIC_PARTITIONS).toCompletableFuture().join().size());
    }
  }

  @Timeout(10)
  @Test
  void testLatest() {
    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertEquals(
          0, admin.latestOffsets(TOPIC_PARTITIONS).toCompletableFuture().join().size());
    }
  }

  @Timeout(10)
  @Test
  void testMaxTimestamp() {
    try (var admin = Admin.of(bootstrapServers())) {
      Assertions.assertEquals(
          0, admin.maxTimestamps(TOPIC_PARTITIONS).toCompletableFuture().join().size());
    }
  }
}
