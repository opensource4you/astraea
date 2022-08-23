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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.broker.LogMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class NodeTopicSizeCostTest {
  private final BeanObject bean =
      new BeanObject(
          "domain", Map.of("topic", "t", "partition", "10", "name", "SIZE"), Map.of("Value", 777));

  @Test
  void testBrokerCost() {
    var meter = new LogMetrics.Log.Gauge(bean);
    var cost = new NodeTopicSizeCost();
    var result =
        cost.brokerCost(mock(ClusterInfo.class), ClusterBean.of(Map.of(1, List.of(meter))));
    Assertions.assertEquals(1, result.value().size());
    Assertions.assertEquals(777, result.value().entrySet().iterator().next().getValue());
  }

  @Test
  void testPartitionCost() {
    var meter = new LogMetrics.Log.Gauge(bean);
    var cost = new NodeTopicSizeCost();
    var clusterInfo = Mockito.mock(ClusterInfo.class);
    when(clusterInfo.topics()).thenReturn(Set.of("t"));
    when(clusterInfo.availableReplicas("t"))
        .thenReturn(List.of(ReplicaInfo.of("t", 10, NodeInfo.of(0, "0", 0), true, false, false)));
    var result = cost.partitionCost(clusterInfo, ClusterBean.of(Map.of(0, List.of(meter))));
    Assertions.assertEquals(1, result.value(0).size());
    Assertions.assertEquals(777, result.value(0).entrySet().iterator().next().getValue());
  }
}
