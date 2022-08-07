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
import java.util.Set;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
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
            ReplicaInfo.of("topic", 0, NodeInfo.of(10, "broker0", 1111), true, true, true),
            ReplicaInfo.of("topic", 0, NodeInfo.of(10, "broker0", 1111), true, true, true),
            ReplicaInfo.of("topic", 0, NodeInfo.of(11, "broker1", 1111), true, true, true));
    var function = new ReplicaLeaderCost.NoMetrics();
    var clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.topics()).thenReturn(Set.of("topic"));
    Mockito.when(clusterInfo.availableReplicaLeaders(Mockito.anyString())).thenReturn(replicas);
    var cost = function.leaderCount(clusterInfo, ClusterBean.EMPTY);
    Assertions.assertTrue(cost.containsKey(10));
    Assertions.assertTrue(cost.containsKey(11));
    Assertions.assertEquals(2, cost.size());
    Assertions.assertTrue(cost.get(10) > cost.get(11));
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
    var brokerLoad = costFunction.brokerCost(ClusterInfo.EMPTY, clusterBean);
    var clusterLoad = costFunction.clusterCost(ClusterInfo.EMPTY, clusterBean);

    Assertions.assertEquals(3, brokerLoad.value().size());
    Assertions.assertEquals(3.0, brokerLoad.value().get(1));
    Assertions.assertEquals(4.0, brokerLoad.value().get(2));
    Assertions.assertEquals(5.0, brokerLoad.value().get(3));
    Assertions.assertEquals(0.2041241452319315, clusterLoad.value());
  }

  private ServerMetrics.ReplicaManager.Meter mockResult(String name, long count) {
    var result = Mockito.mock(ServerMetrics.ReplicaManager.Meter.class);
    var bean = Mockito.mock(BeanObject.class);
    Mockito.when(result.beanObject()).thenReturn(bean);
    Mockito.when(bean.properties()).thenReturn(Map.of("name", name, "type", "ReplicaManager"));
    Mockito.when(result.value()).thenReturn(count);
    return result;
  }
}
