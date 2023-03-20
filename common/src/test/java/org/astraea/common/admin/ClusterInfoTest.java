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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.cost.ReplicaNumberCost;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

public class ClusterInfoTest {

  /**
   * build a cluster info based on replicas. Noted that the node info are collected by the replicas.
   *
   * <p>Be aware that the <code>replicas</code> parameter describes <strong>the replica lists of a
   * subset of topic/partitions</strong>. It doesn't require the topic/partition part to have
   * cluster-wide complete information. But the replica list has to be complete. Provide a partial
   * replica list might result in data loss or unintended replica drop during rebalance plan
   * proposing & execution.
   *
   * @param replicas used to build cluster info
   * @return cluster info
   */
  public static ClusterInfo of(List<Replica> replicas) {
    // TODO: this method is not suitable for production use. Move it to the test scope.
    //  see https://github.com/skiptests/astraea/issues/1185
    return ClusterInfo.of(
        "fake",
        replicas.stream()
            .map(Replica::nodeInfo)
            .collect(Collectors.groupingBy(NodeInfo::id, Collectors.reducing((x, y) -> x)))
            .values()
            .stream()
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableList()),
        Map.of(),
        replicas);
  }

  @Test
  void testEmptyCluster() {
    var emptyCluster = ClusterInfo.empty();
    Assertions.assertEquals(0, emptyCluster.nodes().size());
    Assertions.assertEquals(0, emptyCluster.replicaStream().count());
  }

  @RepeatedTest(3)
  void testTopics() {
    var nodes =
        IntStream.range(0, ThreadLocalRandom.current().nextInt(3, 9))
            .boxed()
            .collect(Collectors.toUnmodifiableSet());
    var topics =
        IntStream.range(0, ThreadLocalRandom.current().nextInt(0, 100))
            .mapToObj(x -> Utils.randomString())
            .collect(Collectors.toUnmodifiableSet());
    var builder =
        ClusterInfoBuilder.builder()
            .addNode(nodes)
            .addFolders(
                nodes.stream()
                    .collect(Collectors.toUnmodifiableMap(x -> x, x -> Set.of("/folder"))));
    topics.forEach(
        t ->
            builder.addTopic(
                t,
                ThreadLocalRandom.current().nextInt(1, 10),
                (short) ThreadLocalRandom.current().nextInt(1, nodes.size())));

    var cluster = builder.build();
    Assertions.assertEquals(topics, cluster.topics().keySet());
    Assertions.assertEquals(topics, cluster.topicNames());
  }

  @Test
  void testReturnCollectionUnmodifiable() {
    var cluster = ClusterInfo.empty();
    var replica =
        Replica.builder()
            .topic("topic")
            .partition(0)
            .nodeInfo(NodeInfo.of(0, "", -1))
            .path("f")
            .buildLeader();
    Assertions.assertThrows(Exception.class, () -> cluster.replicas().add(replica));
    Assertions.assertThrows(Exception.class, () -> cluster.replicas("t").add(replica));
    Assertions.assertThrows(
        Exception.class, () -> cluster.replicas(TopicPartition.of("t", 0)).add(replica));
    Assertions.assertThrows(
        Exception.class, () -> cluster.replicas(TopicPartitionReplica.of("t", 0, 10)).add(replica));
  }

  @Test
  void testChangedReplicaLeaderNumber() {
    var costFunction = new ReplicaLeaderCost();
    var before =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build());
    var after =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build());
    var beforeClusterInfo = ClusterInfoTest.of(before);
    var afterClusterInfo = ClusterInfoTest.of(after);
    var changedReplicaLeaderCount =
        ClusterInfo.changedReplicaNumber(beforeClusterInfo, afterClusterInfo, Replica::isLeader);
    Assertions.assertEquals(3, changedReplicaLeaderCount.size());
    Assertions.assertTrue(changedReplicaLeaderCount.containsKey(0));
    Assertions.assertTrue(changedReplicaLeaderCount.containsKey(1));
    Assertions.assertTrue(changedReplicaLeaderCount.containsKey(2));
    Assertions.assertEquals(-1, changedReplicaLeaderCount.get(0));
    Assertions.assertEquals(0, changedReplicaLeaderCount.get(1));
    Assertions.assertEquals(1, changedReplicaLeaderCount.get(2));
  }

  @Test
  void testChangedReplicaNumber() {
    var costFunction = new ReplicaNumberCost();
    var before =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build());
    var after =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build());
    var beforeClusterInfo = ClusterInfoTest.of(before);
    var afterClusterInfo = ClusterInfoTest.of(after);
    var changedReplicaCount =
        ClusterInfo.changedReplicaNumber(beforeClusterInfo, afterClusterInfo, ignored -> true);
    Assertions.assertEquals(3, changedReplicaCount.size(), changedReplicaCount.toString());
    Assertions.assertTrue(changedReplicaCount.containsKey(0));
    Assertions.assertTrue(changedReplicaCount.containsKey(1));
    Assertions.assertTrue(changedReplicaCount.containsKey(2));
    Assertions.assertEquals(-1, changedReplicaCount.get(0));
    Assertions.assertEquals(-1, changedReplicaCount.get(1));
    Assertions.assertEquals(2, changedReplicaCount.get(2));
  }

  @Test
  void testChangedRecordSize() {

    var moveInResult =
        ClusterInfo.changedRecordSize(
            beforeClusterInfo(), afterClusterInfo(), ignored -> true, false);
    Assertions.assertEquals(3, moveInResult.size());
    Assertions.assertEquals(0, moveInResult.get(0).bytes());
    Assertions.assertEquals(1000, moveInResult.get(1).bytes());
    Assertions.assertEquals(100 + 500, moveInResult.get(2).bytes());

    var moveOutResult =
        ClusterInfo.changedRecordSize(
            afterClusterInfo(), beforeClusterInfo(), ignored -> true, true);
    Assertions.assertEquals(3, moveOutResult.size());
    Assertions.assertEquals(100 + 500, moveOutResult.get(0).bytes());
    Assertions.assertEquals(0, moveOutResult.get(1).bytes());
    Assertions.assertEquals(1000, moveOutResult.get(2).bytes());

    var totalResult =
        ClusterInfo.totalChangedRecordSize(
            beforeClusterInfo(), afterClusterInfo(), ignored -> true);
    Assertions.assertEquals(Math.max(1000 + 100 + 500, 100 + 500 + 1000), totalResult);
  }

  /*
  before distribution:
      p0: 0,1
      p1: 0,1
      p2: 2,0
  after distribution:
      p0: 2,1
      p1: 0,2
      p2: 1,0
  leader log size:
      p0: 100
      p1: 500
      p2  1000
   */
  private static ClusterInfo beforeClusterInfo() {
    return ClusterInfo.of(
        "fake",
        List.of(NodeInfo.of(0, "aa", 22), NodeInfo.of(1, "aa", 22), NodeInfo.of(2, "aa", 22)),
        Map.of(),
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(100)
                .isLeader(true)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(99)
                .isLeader(false)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(500)
                .isLeader(true)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(499)
                .isLeader(false)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(1000)
                .isLeader(true)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(1000)
                .isLeader(false)
                .build()));
  }

  private static ClusterInfo afterClusterInfo() {
    return ClusterInfo.of(
        "fake",
        List.of(NodeInfo.of(0, "aa", 22), NodeInfo.of(1, "aa", 22), NodeInfo.of(2, "aa", 22)),
        Map.of(),
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(100)
                .isLeader(true)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(99)
                .isLeader(false)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(500)
                .isLeader(true)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(500)
                .isLeader(false)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(1000)
                .isLeader(true)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(1000)
                .isLeader(false)
                .build()));
  }
}
