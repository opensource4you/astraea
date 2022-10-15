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
    var result = cost.brokerCost(ClusterInfo.EMPTY, ClusterBean.of(Map.of(1, List.of(meter))));
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
            Replica.of(
                "test-1",
                0,
                NodeInfo.of(0, "", -1),
                -1,
                6000000,
                true,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "test-1",
                0,
                NodeInfo.of(1, "", -1),
                -1,
                6000000,
                false,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "test-1",
                1,
                NodeInfo.of(1, "", -1),
                -1,
                700000,
                true,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "test-1",
                1,
                NodeInfo.of(2, "", -1),
                -1,
                700000,
                false,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "test-2",
                0,
                NodeInfo.of(1, "", -1),
                -1,
                800000,
                true,
                true,
                false,
                false,
                false,
                "/log-path-01"),
            Replica.of(
                "test-2",
                0,
                NodeInfo.of(2, "", -1),
                -1,
                800000,
                false,
                true,
                false,
                false,
                false,
                "/log-path-02"));
    return getClusterInfo(replicas);
  }

  static ClusterInfo<Replica> newClusterInfo() {
    var replicas =
        List.of(
            Replica.of(
                "test-1",
                0,
                NodeInfo.of(0, "", -1),
                -1,
                6000000,
                true,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "test-1",
                0,
                NodeInfo.of(2, "", -1),
                -1,
                6000000,
                false,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "test-1",
                1,
                NodeInfo.of(0, "", -1),
                -1,
                700000,
                true,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "test-1",
                1,
                NodeInfo.of(2, "", -1),
                -1,
                700000,
                false,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "test-2",
                0,
                NodeInfo.of(1, "", -1),
                -1,
                800000,
                true,
                true,
                false,
                false,
                false,
                "/log-path-01"),
            Replica.of(
                "test-2",
                0,
                NodeInfo.of(2, "", -1),
                -1,
                800000,
                false,
                true,
                false,
                false,
                false,
                "/log-path-03"));
    return getClusterInfo(replicas);
  }
}
