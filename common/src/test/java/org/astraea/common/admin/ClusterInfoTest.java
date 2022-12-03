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
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ClusterInfoTest {

  @Test
  void testDiff() {
    /*
    test-1-0 : change only the data folder
    test-1-1 : change the data folder and host
    test-1-2 : no change
    test-2-0 : only change leader, not change the data folder and host
     */
    var beforeReplicas =
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
                .build(),
            Replica.builder()
                .topic("test-2")
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
                .topic("test-2")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(true)
                .path("/data-folder-01")
                .build());
    var afterReplicas =
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
                .path("/data-folder-02")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/data-folder-03")
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
                .build(),
            Replica.builder()
                .topic("test-2")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(true)
                .path("/data-folder-01")
                .build(),
            Replica.builder()
                .topic("test-2")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/data-folder-01")
                .build());
    var nodeInfos = List.of(NodeInfo.of(0, "", -1), NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1));
    var before = ClusterInfo.of(nodeInfos, beforeReplicas);
    var after = ClusterInfo.of(nodeInfos, afterReplicas);
    var changes = ClusterInfo.diff(before, after);

    // test diff in ReplicaLeaderCost
    var leaderMoveCost = new ReplicaLeaderCost().moveCost(before, after, ClusterBean.EMPTY);
    // move 2 leaders
    Assertions.assertEquals(leaderMoveCost.totalCost(), 2);

    Assertions.assertNotEquals(0, after.topics().size());
    Assertions.assertEquals(4, changes.size());
    Assertions.assertEquals(
        1,
        changes.stream()
            .filter(replica -> replica.topic().equals("test-1") && replica.partition() == 0)
            .count());
    Assertions.assertEquals(
        1,
        changes.stream()
            .filter(replica -> replica.topic().equals("test-1") && replica.partition() == 1)
            .count());
    Assertions.assertEquals(
        0,
        changes.stream()
            .filter(replica -> replica.topic().equals("test-1") && replica.partition() == 2)
            .count());
    Assertions.assertEquals(
        2,
        changes.stream()
            .filter(replica -> replica.topic().equals("test-2") && replica.partition() == 0)
            .count());
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

    var clusterInfo = ClusterInfo.of(replicas);
    var maskedClusterInfoHasReplicas = ClusterInfo.masked(clusterInfo, t -> t.equals("test-1"));
    var maskedClusterInfoNoReplicas =
        ClusterInfo.masked(clusterInfo, t -> t.equals("No topic name the same."));

    Assertions.assertNotEquals(0, maskedClusterInfoHasReplicas.nodes().size());
    Assertions.assertNotEquals(0, maskedClusterInfoHasReplicas.replicas().size());
    Assertions.assertEquals(0, maskedClusterInfoNoReplicas.replicas().size());

    Assertions.assertNotEquals(0, clusterInfo.replicaLeaders(0, "test-1").size());
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
