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
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.broker.LogMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReplicaSizeCostTest {
  private final BeanObject bean =
      new BeanObject(
          "domain", Map.of("topic", "t", "partition", "10", "name", "SIZE"), Map.of("Value", 777));

  @Test
  void testBrokerCost() {
    var meter = new LogMetrics.Log.Gauge(bean);
    var cost = new ReplicaSizeCost();
    var result = cost.brokerCost(ClusterInfo.empty(), ClusterBean.of(Map.of(1, List.of(meter))));
    Assertions.assertEquals(1, result.value().size());
    Assertions.assertEquals(777, result.value().entrySet().iterator().next().getValue());
  }

  @Test
  void testMoveCost() {
    var cost = new ReplicaSizeCost();
    var moveCost = cost.moveCost(originClusterInfo(), newClusterInfo(), ClusterBean.EMPTY);
    var totalSize = moveCost.totalCost();
    var changes = moveCost.changes();

    // totalSize will also be calculated for the folder migrate in the same broker.
    Assertions.assertEquals(6000000 + 700000 + 800000, totalSize);
    Assertions.assertEquals(700000, changes.get(0));
    Assertions.assertEquals(-6700000, changes.get(1));
    Assertions.assertEquals(6000000, changes.get(2));
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

  static ClusterInfo<Replica> getClusterInfo(List<Replica> replicas) {
    return ClusterInfo.of(
        Set.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1)),
        List.of(
            replicas.get(0),
            replicas.get(1),
            replicas.get(2),
            replicas.get(3),
            replicas.get(4),
            replicas.get(5)));
  }

  static ClusterInfo<Replica> originClusterInfo() {
    var replicas =
        List.of(
            Replica.builder()
                .topic("test-1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .lag(-1)
                .size(6000000)
                .isLeader(true)
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/log-path-02")
                .build());
    return getClusterInfo(replicas);
  }

  static ClusterInfo<Replica> newClusterInfo() {
    var replicas =
        List.of(
            Replica.builder()
                .topic("test-1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .lag(-1)
                .size(6000000)
                .isLeader(true)
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
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
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/log-path-03")
                .build());
    return getClusterInfo(replicas);
  }

  @Test
  void testPartitionCost() {
    var meter = new LogMetrics.Log.Gauge(bean);
    var cost1 = new ReplicaSizeCost();
    var cost2 = HasPartitionCost.of(Map.of(new ReplicaSizeCost(), (double) 1));
    var result1 =
        cost1.partitionCost(ClusterInfo.empty(), ClusterBean.of(Map.of(0, List.of(meter)))).value();
    var result2 =
        cost2.partitionCost(ClusterInfo.empty(), ClusterBean.of(Map.of(1, List.of(meter)))).value();

    Assertions.assertEquals(1, result1.size());
    Assertions.assertEquals(1, result2.size());
    Assertions.assertEquals(777, result1.get(TopicPartition.of("t", 10)));
    Assertions.assertEquals(777, result2.get(TopicPartition.of("t", 10)));
  }
}
