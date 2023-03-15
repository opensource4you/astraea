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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReplicaLeaderSizeCostTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testClusterCost() {
    final Dispersion dispersion = Dispersion.cov();
    var loadCostFunction = new ReplicaLeaderSizeCost();
    var brokerLoad = loadCostFunction.brokerCost(clusterInfo(), ClusterBean.EMPTY).value();
    var clusterCost = loadCostFunction.clusterCost(clusterInfo(), ClusterBean.EMPTY).value();
    Assertions.assertEquals(dispersion.calculate(brokerLoad.values()), clusterCost);
  }

  @Test
  void testBrokerCost() {
    var cost = new ReplicaLeaderSizeCost();
    var result = cost.brokerCost(clusterInfo(), ClusterBean.EMPTY);
    Assertions.assertEquals(3, result.value().size());
    Assertions.assertEquals(777 + 500, result.value().get(0));
    Assertions.assertEquals(700, result.value().get(1));
    Assertions.assertEquals(500, result.value().get(2));
  }

  @Test
  void testMoveCost() {
    var cost = new ReplicaLeaderSizeCost();
    var moveCost = cost.moveCost(originClusterInfo(), newClusterInfo(), ClusterBean.EMPTY);

    Assertions.assertEquals(
        3, moveCost.movedReplicaLeaderSize().size(), moveCost.movedReplicaLeaderSize().toString());
    Assertions.assertEquals(700000, moveCost.movedReplicaLeaderSize().get(0).bytes());
    Assertions.assertEquals(-700000, moveCost.movedReplicaLeaderSize().get(1).bytes());
    Assertions.assertEquals(6000000, moveCost.movedReplicaLeaderSize().get(2).bytes());
  }

  /*
  origin replica distributed :
    test-1-0 : 0,1
    test-1-1 : 1,2
    test-2-0 : 1,2

  generated plan replica distributed :
    test-1-0 : 0,2
    test-1-1 : 0,2
    test-2-0 : 1,2

   */

  static ClusterInfo getClusterInfo(List<Replica> replicas) {
    return ClusterInfo.of(
        "fake",
        replicas.stream().map(Replica::nodeInfo).collect(Collectors.toList()),
        Map.of(),
        replicas);
  }

  static ClusterInfo originClusterInfo() {
    var replicas =
        List.of(
            Replica.builder()
                .topic("test-1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .lag(-1)
                .size(6000000)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .lag(-1)
                .size(6000000)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(1)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .lag(-1)
                .size(700000)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .lag(-1)
                .size(700000)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-2")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .lag(-1)
                .size(800000)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/log-path-01")
                .build(),
            Replica.builder()
                .topic("test-2")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .lag(-1)
                .size(800000)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/log-path-02")
                .build());
    return getClusterInfo(replicas);
  }

  static ClusterInfo newClusterInfo() {
    var replicas =
        List.of(
            Replica.builder()
                .topic("test-1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .lag(-1)
                .size(6000000)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .lag(-1)
                .size(6000000)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .lag(-1)
                .size(700000)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .lag(-1)
                .size(700000)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-2")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .lag(-1)
                .size(800000)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/log-path-01")
                .build(),
            Replica.builder()
                .topic("test-2")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .lag(-1)
                .size(800000)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/log-path-03")
                .build());
    return getClusterInfo(replicas);
  }

  @Test
  void testPartitionCost() {
    var cost = new ReplicaLeaderSizeCost();
    var result = cost.partitionCost(clusterInfo(), ClusterBean.EMPTY).value();

    Assertions.assertEquals(3, result.size());
    Assertions.assertEquals(777.0, result.get(TopicPartition.of("t", 10)));
    Assertions.assertEquals(700.0, result.get(TopicPartition.of("t", 11)));
    Assertions.assertEquals(500.0, result.get(TopicPartition.of("t", 12)));
  }

  private ClusterInfo clusterInfo() {
    var replicas =
        List.of(
            Replica.builder()
                .topic("t")
                .partition(10)
                .isLeader(true)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .size(777)
                .build(),
            Replica.builder()
                .topic("t")
                .partition(11)
                .isLeader(true)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .size(700)
                .build(),
            Replica.builder()
                .topic("t")
                .partition(12)
                .isLeader(true)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .size(500)
                .build(),
            Replica.builder()
                .topic("t")
                .partition(12)
                .isLeader(false)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .size(499)
                .build());
    return ClusterInfo.of(
        "fake",
        List.of(NodeInfo.of(0, "", -1), NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1)),
        Map.of(),
        replicas);
  }
}
