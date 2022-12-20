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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
   * @param <T> ReplicaInfo or Replica
   */
  public static <T extends ReplicaInfo> ClusterInfo<T> of(List<T> replicas) {
    // TODO: this method is not suitable for production use. Move it to the test scope.
    //  see https://github.com/skiptests/astraea/issues/1185
    return ClusterInfo.of(
        replicas.stream()
            .map(ReplicaInfo::nodeInfo)
            .collect(Collectors.groupingBy(NodeInfo::id, Collectors.reducing((x, y) -> x)))
            .values()
            .stream()
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableList()),
        replicas);
  }

  @Test
  void testReplicaLeadersAndMaskedCluster() {
    var replicas =
        List.of(
            Replica.builder()
                .topic("test-1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/data-folder-01")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(1)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/data-folder-02")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(2)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/data-folder-01")
                .build());

    var clusterInfo = ClusterInfoTest.of(replicas);
    var maskedClusterInfoHasReplicas = ClusterInfo.masked(clusterInfo, t -> t.equals("test-1"));
    var maskedClusterInfoNoReplicas =
        ClusterInfo.masked(clusterInfo, t -> t.equals("No topic name the same."));

    Assertions.assertNotEquals(0, maskedClusterInfoHasReplicas.nodes().size());
    Assertions.assertNotEquals(0, maskedClusterInfoHasReplicas.replicas().size());
    Assertions.assertEquals(0, maskedClusterInfoNoReplicas.replicas().size());

    Assertions.assertNotEquals(0, clusterInfo.replicaLeaders(BrokerTopic.of(0, "test-1")).size());
  }

  @Test
  void testEmptyCluster() {
    var emptyCluster = ClusterInfo.empty();
    Assertions.assertEquals(0, emptyCluster.nodes().size());
    Assertions.assertEquals(0, emptyCluster.replicaStream().count());
  }

  @Test
  void testNode() {
    var node = NodeInfoTest.node();
    var partition = ReplicaInfoTest.partitionInfo();
    var kafkaCluster = Mockito.mock(Cluster.class);
    Mockito.when(kafkaCluster.topics()).thenReturn(Set.of(partition.topic()));
    Mockito.when(kafkaCluster.availablePartitionsForTopic(partition.topic()))
        .thenReturn(List.of(partition));
    Mockito.when(kafkaCluster.partitionsForTopic(partition.topic())).thenReturn(List.of(partition));
    Mockito.when(kafkaCluster.nodes()).thenReturn(List.of(node));

    var clusterInfo = ClusterInfo.of(kafkaCluster);

    Assertions.assertEquals(1, clusterInfo.nodes().size());
    Assertions.assertEquals(NodeInfo.of(node), clusterInfo.nodes().iterator().next());
    Assertions.assertEquals(1, clusterInfo.availableReplicas(partition.topic()).size());
    Assertions.assertEquals(1, clusterInfo.replicas(partition.topic()).size());
    Assertions.assertEquals(
        NodeInfo.of(node), clusterInfo.availableReplicas(partition.topic()).get(0).nodeInfo());
    Assertions.assertEquals(
        NodeInfo.of(node), clusterInfo.replicaLeaders(partition.topic()).get(0).nodeInfo());
    Assertions.assertEquals(
        NodeInfo.of(node), clusterInfo.replicas(partition.topic()).get(0).nodeInfo());
  }

  @Test
  void testIllegalQuery() {
    var clusterInfo = ClusterInfo.of(Cluster.empty());
    Assertions.assertEquals(0, clusterInfo.replicas("unknown").size());
    Assertions.assertThrows(NoSuchElementException.class, () -> clusterInfo.node(0));
    Assertions.assertEquals(0, clusterInfo.availableReplicas("unknown").size());
    Assertions.assertEquals(0, clusterInfo.replicaLeaders("unknown").size());
  }
}
