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
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.ClusterBean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicaLeaderCostTest {
  private final Dispersion dispersion = Dispersion.standardDeviation();

  @Test
  void testLeaderCount() {
    var baseCluster =
        ClusterInfo.builder()
            .addNode(Set.of(1, 2))
            .addFolders(Map.of(1, Set.of("/folder")))
            .addFolders(Map.of(2, Set.of("/folder")))
            .build();
    var sourceCluster =
        ClusterInfo.builder(baseCluster)
            .addTopic(
                "topic1", 3, (short) 1, r -> Replica.builder(r).broker(baseCluster.node(1)).build())
            .addTopic(
                "topic2", 3, (short) 1, r -> Replica.builder(r).broker(baseCluster.node(2)).build())
            .build();
    var overFlowTargetCluster =
        ClusterInfo.builder(baseCluster)
            .addTopic(
                "topic1", 3, (short) 1, r -> Replica.builder(r).broker(baseCluster.node(2)).build())
            .addTopic(
                "topic2", 3, (short) 1, r -> Replica.builder(r).broker(baseCluster.node(1)).build())
            .build();

    var overFlowMoveCost =
        new ReplicaLeaderCost(
                new Configuration(Map.of(ReplicaLeaderCost.MAX_MIGRATE_LEADER_KEY, "5")))
            .moveCost(sourceCluster, overFlowTargetCluster, ClusterBean.EMPTY);

    var noOverFlowMoveCost =
        new ReplicaLeaderCost(
                new Configuration(Map.of(ReplicaLeaderCost.MAX_MIGRATE_LEADER_KEY, "10")))
            .moveCost(sourceCluster, overFlowTargetCluster, ClusterBean.EMPTY);

    Assertions.assertTrue(overFlowMoveCost.overflow());
    Assertions.assertFalse(noOverFlowMoveCost.overflow());
  }

  @Test
  void testNoMetrics() {
    var replicas =
        List.of(
            Replica.builder()
                .topic("topic")
                .partition(0)
                .isLeader(true)
                .broker(Broker.of(10, "broker0", 1111))
                .path("/tmp/aa")
                .buildLeader(),
            Replica.builder()
                .topic("topic")
                .partition(1)
                .isLeader(true)
                .partition(0)
                .broker(Broker.of(10, "broker0", 1111))
                .path("/tmp/aa")
                .buildLeader(),
            Replica.builder()
                .topic("topic")
                .partition(0)
                .isLeader(true)
                .broker(Broker.of(11, "broker1", 1111))
                .path("/tmp/aa")
                .buildLeader());
    var clusterInfo =
        ClusterInfo.of(
            "fake",
            List.of(
                Broker.of(10, "host1", 8080),
                Broker.of(11, "host1", 8080),
                Broker.of(12, "host1", 8080)),
            Map.of(),
            replicas);
    var brokerCost = ReplicaLeaderCost.leaderCount(clusterInfo);
    var cf = new ReplicaLeaderCost();
    var leaderNum = brokerCost.values().stream().mapToInt(x -> x).sum();
    var normalizedScore =
        brokerCost.values().stream()
            .map(score -> (double) score / leaderNum)
            .collect(Collectors.toList());
    var clusterCost = dispersion.calculate(normalizedScore) * 2;
    Assertions.assertTrue(brokerCost.containsKey(10));
    Assertions.assertTrue(brokerCost.containsKey(11));
    Assertions.assertEquals(3, brokerCost.size());
    Assertions.assertTrue(brokerCost.get(10) > brokerCost.get(11));
    Assertions.assertEquals(brokerCost.get(12), 0);
    Assertions.assertEquals(clusterCost, cf.clusterCost(clusterInfo, ClusterBean.EMPTY).value());
  }
}
