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
package org.astraea.common.balancer.log;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ClusterLogAllocationTest extends RequireBrokerCluster {

  private Set<TopicPartition> generateRandomTopicPartition() {
    return IntStream.range(0, 30)
        .mapToObj(i -> "topic-" + i)
        .map(topic -> TopicPartition.of(topic, ThreadLocalRandom.current().nextInt(0, 100)))
        .limit(30)
        .collect(Collectors.toUnmodifiableSet());
  }

  private Set<Replica> generateFakeReplica(TopicPartition tp, int replicas) {
    return IntStream.range(0, replicas)
        .mapToObj(
            rIndex ->
                Replica.of(
                    tp.topic(),
                    tp.partition(),
                    NodeInfo.of(rIndex, "hostname" + rIndex, 9092),
                    0,
                    ThreadLocalRandom.current().nextInt(0, 30000000),
                    rIndex == 0,
                    true,
                    false,
                    false,
                    rIndex == 0,
                    "/tmp/dir0"))
        .collect(Collectors.toUnmodifiableSet());
  }

  private List<Replica> generateRandomReplicaList(
      Set<TopicPartition> topicPartitions, short replicas) {
    return topicPartitions.stream()
        .flatMap(tp -> generateFakeReplica(tp, replicas).stream())
        .collect(Collectors.toUnmodifiableList());
  }

  private Replica update(Replica baseReplica, Map<String, Object> override) {
    return Replica.of(
        (String) override.getOrDefault("topic", baseReplica.topic()),
        (int) override.getOrDefault("partition", baseReplica.partition()),
        NodeInfo.of((int) override.getOrDefault("broker", baseReplica.nodeInfo().id()), "", -1),
        (long) override.getOrDefault("size", baseReplica.size()),
        (long) override.getOrDefault("lag", baseReplica.lag()),
        (boolean) override.getOrDefault("leader", baseReplica.isLeader()),
        (boolean) override.getOrDefault("synced", baseReplica.inSync()),
        (boolean) override.getOrDefault("future", baseReplica.isFuture()),
        (boolean) override.getOrDefault("offline", baseReplica.isOffline()),
        (boolean) override.getOrDefault("preferred", baseReplica.isPreferredLeader()),
        (String) override.getOrDefault("dir", "/tmp/default/dir"));
  }

  @ParameterizedTest
  @DisplayName("Create CLA from ClusterInfo")
  @ValueSource(shorts = {1, 2, 3})
  void testOfClusterInfo(short replicas) throws ExecutionException, InterruptedException {
    // arrange
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var topic0 = Utils.randomString();
      var topic1 = Utils.randomString();
      var topic2 = Utils.randomString();
      var topics = Set.of(topic0, topic1, topic2);
      var partitions = ThreadLocalRandom.current().nextInt(10, 30);
      topics.forEach(
          topic ->
              admin
                  .creator()
                  .topic(topic)
                  .numberOfPartitions(partitions)
                  .numberOfReplicas(replicas)
                  .create());
      Utils.sleep(Duration.ofSeconds(1));

      // act
      final var cla =
          ClusterLogAllocation.of(admin.clusterInfo(topics).toCompletableFuture().get());

      // assert
      final var expectedPartitions =
          topics.stream()
              .flatMap(
                  topic ->
                      IntStream.range(0, partitions).mapToObj(p -> TopicPartition.of(topic, p)))
              .collect(Collectors.toUnmodifiableSet());
      Assertions.assertEquals(expectedPartitions, cla.topicPartitions());
      Assertions.assertEquals(topics.size() * partitions * replicas, cla.logPlacements().size());
      expectedPartitions.forEach(
          tp -> Assertions.assertEquals(replicas, cla.logPlacements(tp).size()));
    }
  }

  @ParameterizedTest
  @DisplayName("Create CLA from replica list")
  @ValueSource(shorts = {1, 2, 3, 4, 5, 30})
  void testOfReplicaList(short replicas) {
    // arrange
    final var randomTopicPartitions = generateRandomTopicPartition();
    final var randomReplicas = generateRandomReplicaList(randomTopicPartitions, replicas);

    // act
    final var cla = ClusterLogAllocation.of(randomReplicas);

    // assert
    Assertions.assertEquals(randomReplicas.size(), cla.logPlacements().size());
    Assertions.assertEquals(Set.copyOf(randomReplicas), cla.logPlacements());
    Assertions.assertEquals(randomTopicPartitions, cla.topicPartitions());
  }

  @Test
  void testOfBadReplicaList() {
    // arrange
    var topic = Utils.randomString();
    var partition = 30;
    var nodeInfo = NodeInfo.of(0, "", -1);
    Replica base =
        Replica.of(
            topic, partition, nodeInfo, 0, 0, true, false, false, false, true, "/tmp/default/dir");
    Replica leader0 = update(base, Map.of("broker", 3, "preferred", true));
    Replica leader1 = update(base, Map.of("broker", 4, "preferred", true));
    Replica follower2 = update(base, Map.of("broker", 4, "preferred", false));

    // act, assert
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ClusterLogAllocation.of(List.of(leader0, leader1)),
        "duplicate preferred leader in list");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ClusterLogAllocation.of(List.of(follower2)),
        "no preferred leader");
  }

  @Test
  @DisplayName("Migrate some replicas without directory specified")
  void testMigrateReplicaNoDir() {
    // arrange
    final var randomTopicPartitions = generateRandomTopicPartition();
    final var randomReplicas = generateRandomReplicaList(randomTopicPartitions, (short) 3);
    final var allocation = ClusterLogAllocation.of(randomReplicas);
    final var target = randomReplicas.stream().findAny().orElseThrow();

    // act, assert
    Assertions.assertThrows(
        NullPointerException.class,
        () -> allocation.migrateReplica(target.topicPartitionReplica(), 9999, null));
  }

  @ParameterizedTest
  @DisplayName("Migrate some replicas")
  @ValueSource(shorts = {1, 2, 3, 4, 5, 30})
  void testMigrateReplica(short replicas) {
    // arrange
    final var randomTopicPartitions = generateRandomTopicPartition();
    final var randomReplicas = generateRandomReplicaList(randomTopicPartitions, replicas);
    final var allocation = ClusterLogAllocation.of(randomReplicas);
    final var target = randomReplicas.stream().findAny().orElseThrow();

    // act
    final var cla = allocation.migrateReplica(target.topicPartitionReplica(), 9999, "/the/dir");

    // assert
    Assertions.assertEquals(
        replicas, cla.logPlacements(target.topicPartition()).size(), "No replica factor shrinkage");
    Assertions.assertTrue(
        cla.logPlacements(target.topicPartition()).stream()
            .anyMatch(replica -> replica.nodeInfo().id() == 9999),
        "The replica is here");
    Assertions.assertTrue(
        cla.logPlacements(target.topicPartition()).stream()
            .noneMatch(replica -> replica.nodeInfo().id() == target.nodeInfo().id()),
        "The original replica is gone");
    Assertions.assertEquals(
        "/the/dir",
        cla.logPlacements(target.topicPartition()).stream()
            .filter(replica -> replica.nodeInfo().id() == 9999)
            .findFirst()
            .orElseThrow()
            .path());
  }

  @ParameterizedTest
  @DisplayName("Become leader")
  @ValueSource(shorts = {1, 2, 3, 4, 5, 30})
  void testBecomeLeader(short replicas) {
    // arrange
    final var randomTopicPartitions = generateRandomTopicPartition();
    final var randomReplicas = generateRandomReplicaList(randomTopicPartitions, replicas);
    final var allocation = ClusterLogAllocation.of(randomReplicas);
    final var target = randomReplicas.stream().findAny().orElseThrow();
    final var theTopicPartition = target.topicPartition();
    final var originalReplicaList = allocation.logPlacements(theTopicPartition);
    final var originalLeader =
        originalReplicaList.stream().filter(Replica::isPreferredLeader).findFirst().orElseThrow();

    // act
    final var cla = allocation.becomeLeader(target.topicPartitionReplica());

    // assert
    Assertions.assertEquals(
        replicas, cla.logPlacements(theTopicPartition).size(), "No replica factor shrinkage");
    if (target.isLeader()) {
      // let leader become a leader, nothing changed
      Assertions.assertEquals(
          originalReplicaList,
          allocation.logPlacements(theTopicPartition),
          "Nothing changed since target is already the leader");
    } else {
      Assertions.assertTrue(
          cla.logPlacements(theTopicPartition).stream()
              .filter(r -> r.nodeInfo().equals(target.nodeInfo()))
              .findFirst()
              .orElseThrow()
              .isPreferredLeader(),
          "target become the new preferred leader");
      Assertions.assertFalse(
          cla.logPlacements(theTopicPartition).stream()
              .filter(r -> r.nodeInfo().equals(originalLeader.nodeInfo()))
              .findFirst()
              .orElseThrow()
              .isPreferredLeader(),
          "original leader lost its identity");
      Assertions.assertEquals(
          target.size(),
          cla.logPlacements(theTopicPartition).stream()
              .filter(r -> r.nodeInfo().equals(target.nodeInfo()))
              .findFirst()
              .orElseThrow()
              .size(),
          "Only the preferred leader field get updated, no change to other fields");
      Assertions.assertEquals(
          originalLeader.size(),
          cla.logPlacements(theTopicPartition).stream()
              .filter(r -> r.nodeInfo().equals(originalLeader.nodeInfo()))
              .findFirst()
              .orElseThrow()
              .size(),
          "Only the preferred leader field get updated, no change to other fields");
    }
  }

  @ParameterizedTest
  @DisplayName("placements")
  @ValueSource(shorts = {1, 2, 3, 4, 5, 30})
  void testLogPlacements(short replicas) {
    // arrange
    final var randomTopicPartitions = generateRandomTopicPartition();
    final var randomReplicas = generateRandomReplicaList(randomTopicPartitions, replicas);

    // act
    final var allocation = ClusterLogAllocation.of(randomReplicas);

    // assert
    Assertions.assertEquals(Set.copyOf(randomReplicas), allocation.logPlacements());
    for (var tp : randomTopicPartitions) {
      final var expected =
          randomReplicas.stream()
              .filter(r -> r.topicPartition().equals(tp))
              .collect(Collectors.toUnmodifiableSet());
      Assertions.assertEquals(expected, allocation.logPlacements(tp));
    }
  }

  @ParameterizedTest
  @DisplayName("placements")
  @ValueSource(shorts = {1, 2, 3, 4, 5, 30})
  void testTopicPartitions(short replicas) {
    // arrange
    final var randomTopicPartitions = generateRandomTopicPartition();
    final var randomReplicas = generateRandomReplicaList(randomTopicPartitions, replicas);

    // act
    final var allocation = ClusterLogAllocation.of(randomReplicas);

    // assert
    Assertions.assertEquals(randomTopicPartitions, allocation.topicPartitions());
  }

  @Test
  void testPlacementMatch() {
    var topic = Utils.randomString();
    var partition = 30;
    var nodeInfo = NodeInfo.of(0, "", -1);

    Replica base =
        Replica.of(
            topic,
            partition,
            nodeInfo,
            0,
            0,
            false,
            false,
            false,
            false,
            false,
            "/tmp/default/dir");

    {
      // self equal
      var leader0 = update(base, Map.of("leader", true, "preferred", true));
      var follower1 = update(base, Map.of("broker", 1));
      var follower2 = update(base, Map.of("broker", 2));
      Assertions.assertTrue(
          ClusterLogAllocation.placementMatch(
              Set.of(leader0, follower1, follower2), Set.of(leader0, follower1, follower2)),
          "Self equal");
    }

    {
      // unrelated field does nothing
      var leader0 = update(base, Map.of("leader", true, "preferred", true));
      var follower1 = update(base, Map.of("broker", 1));
      var follower2 = update(base, Map.of("broker", 2));
      var awkwardFollower2 = update(follower2, Map.of("size", 123456789L));
      Assertions.assertTrue(
          ClusterLogAllocation.placementMatch(
              Set.of(leader0, follower1, follower2), Set.of(leader0, follower1, awkwardFollower2)),
          "Size field is unrelated to placement");
    }

    {
      // preferred leader changed
      var leaderA0 = update(base, Map.of("leader", true, "preferred", true));
      var followerA1 = update(base, Map.of("broker", 1));
      var followerA2 = update(base, Map.of("broker", 2));
      var followerB0 = update(leaderA0, Map.of("preferred", false));
      var leaderB1 = update(followerA1, Map.of("preferred", true));
      var followerB2 = update(followerA2, Map.of());
      Assertions.assertFalse(
          ClusterLogAllocation.placementMatch(
              Set.of(leaderA0, followerA1, followerA2), Set.of(leaderB1, followerB0, followerB2)),
          "Size field is unrelated to placement");
    }

    {
      // data dir changed
      var leader0 = update(base, Map.of("leader", true, "preferred", true));
      var follower1 = update(base, Map.of("broker", 1));
      var follower2 = update(base, Map.of("broker", 2));
      var alteredFollower2 = update(follower2, Map.of("dir", "/tmp/somewhere"));
      Assertions.assertFalse(
          ClusterLogAllocation.placementMatch(
              Set.of(leader0, follower1, follower2), Set.of(leader0, follower1, alteredFollower2)),
          "data dir changed");
    }

    {
      // replica migrated
      var leader0 = update(base, Map.of("leader", true, "preferred", true));
      var follower1 = update(base, Map.of("broker", 1));
      var follower2 = update(base, Map.of("broker", 2));
      var alteredFollower2 = update(follower2, Map.of("broker", 3));
      Assertions.assertFalse(
          ClusterLogAllocation.placementMatch(
              Set.of(leader0, follower1, follower2), Set.of(leader0, follower1, alteredFollower2)),
          "migrate data dir");
    }

    {
      // null dir in target set mean always bad
      var leader0 = update(base, Map.of("leader", true, "preferred", true));
      var follower1 = update(base, Map.of("broker", 1));
      var nullMap = new HashMap<String, Object>();
      nullMap.put("dir", null);
      var sourceFollower1 = update(follower1, Map.of("dir", "/target"));
      var targetFollower1 = update(follower1, nullMap);
      Assertions.assertFalse(
          ClusterLogAllocation.placementMatch(
              Set.of(leader0, sourceFollower1), Set.of(leader0, targetFollower1)));
    }
  }

  @Test
  void testFindNonFulfilledAllocation() {
    var topic = Utils.randomString();
    var partition = 30;
    var nodeInfo = NodeInfo.of(0, "", -1);

    Replica baseLeader =
        Replica.of(
            topic, partition, nodeInfo, 0, 0, true, false, false, false, true, "/tmp/default/dir");

    {
      // self equal
      var leader0 = update(baseLeader, Map.of());
      var follower1 = update(baseLeader, Map.of("broker", 1, "leader", false, "preferred", false));
      var follower2 = update(baseLeader, Map.of("broker", 2, "leader", false, "preferred", false));
      Assertions.assertEquals(
          Set.of(),
          ClusterLogAllocation.findNonFulfilledAllocation(
              ClusterLogAllocation.of(List.of(leader0, follower1, follower2)),
              ClusterLogAllocation.of(List.of(leader0, follower1, follower2))));
    }

    {
      // one alteration
      var leader0 = update(baseLeader, Map.of());
      var follower1 = update(baseLeader, Map.of("broker", 1, "leader", false, "preferred", false));
      var follower2 = update(baseLeader, Map.of("broker", 2, "leader", false, "preferred", false));
      var alteredFollower2 = update(follower2, Map.of("broker", 3));
      Assertions.assertEquals(
          Set.of(alteredFollower2.topicPartition()),
          ClusterLogAllocation.findNonFulfilledAllocation(
              ClusterLogAllocation.of(List.of(leader0, follower1, follower2)),
              ClusterLogAllocation.of(List.of(leader0, follower1, alteredFollower2))));
    }

    {
      // two alteration
      var leader0 = update(baseLeader, Map.of());
      var leader1 = update(baseLeader, Map.of("topic", "BBB"));
      var leader2 = update(baseLeader, Map.of("topic", "CCC"));
      var alteredLeader1 = update(baseLeader, Map.of("topic", "BBB", "broker", 4));
      var alteredLeader2 = update(baseLeader, Map.of("topic", "CCC", "broker", 5));
      Assertions.assertEquals(
          Set.of(alteredLeader1.topicPartition(), alteredLeader2.topicPartition()),
          ClusterLogAllocation.findNonFulfilledAllocation(
              ClusterLogAllocation.of(List.of(leader0, leader1, leader2)),
              ClusterLogAllocation.of(List.of(leader0, alteredLeader1, alteredLeader2))));
    }
  }

  @Test
  @DisplayName("the source CLA should be a subset of target CLA")
  void testFindNonFulfilledAllocationException() {
    var topic = Utils.randomString();
    var partition = 30;
    var nodeInfo = NodeInfo.of(0, "", -1);

    Replica baseLeader =
        Replica.of(
            topic, partition, nodeInfo, 0, 0, true, false, false, false, true, "/tmp/default/dir");

    {
      var leader0 = update(baseLeader, Map.of());
      var follower1 = update(baseLeader, Map.of("broker", 1, "leader", false, "preferred", false));
      var follower2 = update(baseLeader, Map.of("broker", 2, "leader", false, "preferred", false));
      var other = update(baseLeader, Map.of("topic", "AnotherTopic"));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              ClusterLogAllocation.findNonFulfilledAllocation(
                  ClusterLogAllocation.of(List.of(leader0, follower1, follower2)),
                  ClusterLogAllocation.of(List.of(leader0, follower1, follower2, other))));
    }
  }

  @Test
  void testUpdate() {
    var topic = Utils.randomString();
    var partition = 30;
    var nodeInfo = NodeInfo.of(0, "", -1);
    var newNodeInfo = NodeInfo.of(1, "", -1);
    var lag = 100L;
    var size = 200L;

    Replica replica =
        Replica.of(
            topic,
            partition,
            nodeInfo,
            lag,
            size,
            true,
            false,
            false,
            false,
            true,
            "/tmp/default/dir");

    Assertions.assertEquals(
        "/other", ClusterLogAllocation.update(replica, nodeInfo.id(), "/other").path());
    Assertions.assertEquals(
        nodeInfo, ClusterLogAllocation.update(replica, nodeInfo.id(), "/other").nodeInfo());
    Assertions.assertEquals(
        5566, ClusterLogAllocation.update(replica, 5566, "/other").nodeInfo().id());
    Assertions.assertEquals(
        "/other", ClusterLogAllocation.update(replica, newNodeInfo, "/other").path());
    Assertions.assertEquals(
        newNodeInfo, ClusterLogAllocation.update(replica, newNodeInfo, "/other").nodeInfo());
    Assertions.assertEquals(
        100L, ClusterLogAllocation.update(replica, newNodeInfo, "/other").lag());
    Assertions.assertEquals(
        200L, ClusterLogAllocation.update(replica, newNodeInfo, "/other").size());
  }
}
