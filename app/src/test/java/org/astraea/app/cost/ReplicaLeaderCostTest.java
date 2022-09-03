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
package org.astraea.app.cost;

import java.util.List;
import java.util.Map;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.broker.ServerMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ReplicaLeaderCostTest {

  @Test
  void testNoMetrics() {
    var replicas =
        List.of(
            ReplicaInfo.of("topic", 0, NodeInfo.of(10, "broker0", 1111), true, true, false),
            ReplicaInfo.of("topic", 0, NodeInfo.of(10, "broker0", 1111), true, true, false),
            ReplicaInfo.of("topic", 0, NodeInfo.of(11, "broker1", 1111), true, true, false));
    var clusterInfo = ClusterInfo.of(replicas);
    var brokerCost = ReplicaLeaderCost.leaderCount(clusterInfo);

    Assertions.assertTrue(brokerCost.containsKey(10));
    Assertions.assertTrue(brokerCost.containsKey(11));
    Assertions.assertEquals(2, brokerCost.size());
    Assertions.assertTrue(brokerCost.get(10) > brokerCost.get(11));
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
            Replica.of(
                "topic1",
                0,
                NodeInfo.of(0, "broker0", 1111),
                -1,
                -1,
                true,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "topic1",
                0,
                NodeInfo.of(1, "broker0", 1111),
                -1,
                -1,
                false,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "topic1",
                1,
                NodeInfo.of(0, "broker0", 1111),
                -1,
                -1,
                true,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "topic1",
                1,
                NodeInfo.of(1, "broker0", 1111),
                -1,
                -1,
                false,
                true,
                false,
                false,
                false,
                ""));
    var after =
        List.of(
            Replica.of(
                "topic1",
                0,
                NodeInfo.of(2, "broker0", 1111),
                -1,
                -1,
                true,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "topic1",
                0,
                NodeInfo.of(1, "broker0", 1111),
                -1,
                -1,
                false,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "topic1",
                1,
                NodeInfo.of(0, "broker0", 1111),
                -1,
                -1,
                true,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "topic1",
                1,
                NodeInfo.of(2, "broker0", 1111),
                -1,
                -1,
                false,
                true,
                false,
                false,
                false,
                ""));
    var beforeClusterInfo = ClusterInfo.of(before);
    var afterClusterInfo = ClusterInfo.of(after);
    var movecost = costFunction.moveCost(beforeClusterInfo, afterClusterInfo, ClusterBean.EMPTY);
    Assertions.assertEquals(1, movecost.totalCost());
    Assertions.assertEquals(2, movecost.changes().size());
    Assertions.assertTrue(movecost.changes().containsKey(0));
    Assertions.assertTrue(movecost.changes().containsKey(2));
    Assertions.assertEquals(-1, movecost.changes().get(0));
    Assertions.assertEquals(1, movecost.changes().get(2));
  }

  private ServerMetrics.ReplicaManager.Gauge mockResult(String name, long count) {
    var result = Mockito.mock(ServerMetrics.ReplicaManager.Gauge.class);
    var bean = Mockito.mock(BeanObject.class);
    Mockito.when(result.beanObject()).thenReturn(bean);
    Mockito.when(bean.properties()).thenReturn(Map.of("name", name, "type", "ReplicaManager"));
    Mockito.when(result.value()).thenReturn(count);
    return result;
  }
}
