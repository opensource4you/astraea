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
package org.astraea.common.cost;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoTest;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReplicaNumberCostTest {

  @Test
  void testClusterCostWhenNodeHavingNothing() {
    var nodeInfo = NodeInfo.of(10, "h", 100);
    var replica = Replica.builder().nodeInfo(nodeInfo).topic("t").build();
    var clusterInfo =
        ClusterInfo.of(List.of(nodeInfo, NodeInfo.of(100, "h", 100)), List.of(replica));
    var cost = new ReplicaNumberCost();
    Assertions.assertEquals(
        Long.MAX_VALUE, cost.clusterCost(clusterInfo, ClusterBean.EMPTY).value());
  }

  @Test
  void testClusterCostForSingleNode() {
    var nodeInfo = NodeInfo.of(10, "h", 100);
    var clusterInfo = ClusterInfo.of(List.of(nodeInfo), List.<Replica>of());
    var cost = new ReplicaNumberCost();
    Assertions.assertEquals(0, cost.clusterCost(clusterInfo, ClusterBean.EMPTY).value());
  }

  @Test
  void testClusterCostForAllInSingleNode() {
    var nodeInfo = NodeInfo.of(10, "h", 100);
    var replica = Replica.builder().nodeInfo(nodeInfo).topic("t").build();
    var clusterInfo =
        ClusterInfo.of(List.of(nodeInfo, NodeInfo.of(11, "j", 100)), List.of(replica));
    var cost = new ReplicaNumberCost();
    Assertions.assertEquals(
        Long.MAX_VALUE, cost.clusterCost(clusterInfo, ClusterBean.EMPTY).value());
  }

  @Test
  void testClusterCost() {
    var nodeInfo = NodeInfo.of(10, "h", 100);
    var replicas =
        IntStream.range(0, 10)
            .mapToObj(i -> Replica.builder().nodeInfo(nodeInfo).topic("t").build())
            .collect(Collectors.toCollection(ArrayList::new));
    replicas.add(Replica.builder().nodeInfo(NodeInfo.of(11, "j", 100)).topic("t").build());
    var clusterInfo = ClusterInfo.of(List.of(nodeInfo, NodeInfo.of(11, "j", 100)), replicas);
    var cost = new ReplicaNumberCost();
    Assertions.assertEquals(9, cost.clusterCost(clusterInfo, ClusterBean.EMPTY).value());
  }

  @Test
  void testMoveCost() {
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
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build());
    var beforeClusterInfo = ClusterInfoTest.of(before);
    var afterClusterInfo = ClusterInfoTest.of(after);
    var movecost = costFunction.moveCost(beforeClusterInfo, afterClusterInfo, ClusterBean.EMPTY);
    Assertions.assertEquals(2, movecost.totalCost());
    Assertions.assertEquals(3, movecost.changes().size());
    Assertions.assertTrue(movecost.changes().containsKey(0));
    Assertions.assertTrue(movecost.changes().containsKey(1));
    Assertions.assertTrue(movecost.changes().containsKey(2));
    Assertions.assertEquals(-1, movecost.changes().get(0));
    Assertions.assertEquals(-1, movecost.changes().get(1));
    Assertions.assertEquals(2, movecost.changes().get(2));
  }
}
