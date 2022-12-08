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
import org.astraea.common.admin.ClusterInfoTest;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ReplicaLeaderCostTest {
  private final Dispersion dispersion = Dispersion.correlationCoefficient();

  @Test
  void testNoMetrics() {
    var replicas =
        List.of(
            ReplicaInfo.of("topic", 0, NodeInfo.of(10, "broker0", 1111), true, true, false),
            ReplicaInfo.of("topic", 0, NodeInfo.of(10, "broker0", 1111), true, true, false),
            ReplicaInfo.of("topic", 0, NodeInfo.of(11, "broker1", 1111), true, true, false));
    var clusterInfo =
        ClusterInfo.of(
            List.of(
                NodeInfo.of(10, "host1", 8080),
                NodeInfo.of(11, "host1", 8080),
                NodeInfo.of(12, "host1", 8080)),
            replicas);
    var brokerCost = ReplicaLeaderCost.leaderCount(clusterInfo);
    var clusterCost =
        dispersion.calculate(
            brokerCost.values().stream().map(x -> (double) x).collect(Collectors.toSet()));
    Assertions.assertTrue(brokerCost.containsKey(10));
    Assertions.assertTrue(brokerCost.containsKey(11));
    Assertions.assertEquals(3, brokerCost.size());
    Assertions.assertTrue(brokerCost.get(10) > brokerCost.get(11));
    Assertions.assertEquals(brokerCost.get(12), 0);
    Assertions.assertEquals(clusterCost, 0.816496580927726);
  }

  @Test
  void testWithMetrics() {
    var costFunction = new ReplicaLeaderCost();
    var LeaderCount1 = mockResult(ServerMetrics.ReplicaManager.LEADER_COUNT.metricName(), 3);
    var LeaderCount2 = mockResult(ServerMetrics.ReplicaManager.LEADER_COUNT.metricName(), 4);
    var LeaderCount3 = mockResult(ServerMetrics.ReplicaManager.LEADER_COUNT.metricName(), 5);

    var broker1 = List.of((HasBeanObject) LeaderCount1);
    var broker2 = List.of((HasBeanObject) LeaderCount2);
    var broker3 = List.of((HasBeanObject) LeaderCount3);
    var clusterBean = ClusterBean.of(Map.of(1, broker1, 2, broker2, 3, broker3));
    var brokerLoad = costFunction.brokerCost(ClusterInfo.empty(), clusterBean);
    var clusterLoad = costFunction.clusterCost(ClusterInfo.empty(), clusterBean);

    Assertions.assertEquals(3, brokerLoad.value().size());
    Assertions.assertEquals(3.0, brokerLoad.value().get(1));
    Assertions.assertEquals(4.0, brokerLoad.value().get(2));
    Assertions.assertEquals(5.0, brokerLoad.value().get(3));
    Assertions.assertEquals(0.2041241452319315, clusterLoad.value());
  }

  @Test
  void testMoveCost() {
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
    var moveCost = costFunction.moveCost(beforeClusterInfo, afterClusterInfo, ClusterBean.EMPTY);
    Assertions.assertEquals(1, moveCost.totalCost());
    Assertions.assertEquals(2, moveCost.changes().size());
    Assertions.assertTrue(moveCost.changes().containsKey(0));
    Assertions.assertTrue(moveCost.changes().containsKey(2));
    Assertions.assertEquals(-1, moveCost.changes().get(0));
    Assertions.assertEquals(1, moveCost.changes().get(2));
  }

  private ServerMetrics.ReplicaManager.Gauge mockResult(String name, int count) {
    var result = Mockito.mock(ServerMetrics.ReplicaManager.Gauge.class);
    var bean = Mockito.mock(BeanObject.class);
    Mockito.when(result.beanObject()).thenReturn(bean);
    Mockito.when(bean.properties()).thenReturn(Map.of("name", name, "type", "ReplicaManager"));
    Mockito.when(result.value()).thenReturn(count);
    return result;
  }
}
