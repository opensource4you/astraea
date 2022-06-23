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
import org.astraea.app.metrics.kafka.BrokerTopicMetricsResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ThroughputCostTest {

  @Test
  void testScore() {
    var throughputCost = new ThroughputCost();

    var node = NodeInfo.of(10, "host", 1000);
    var bean = Mockito.mock(BrokerTopicMetricsResult.class);
    Mockito.when(bean.oneMinuteRate()).thenReturn(100D);

    var score = throughputCost.score(Map.of(10, List.of(bean)));
    Assertions.assertEquals(1, score.size());
    Assertions.assertEquals(100D, score.get(10));
  }

  @Test
  void testCost() {
    var throughputCost = new ThroughputCost();

    var node = NodeInfo.of(10, "host", 1000);
    var bean = Mockito.mock(BrokerTopicMetricsResult.class);
    Mockito.when(bean.oneMinuteRate()).thenReturn(100D);

    var cluster = Mockito.mock(ClusterInfo.class);
    Mockito.when(cluster.nodes()).thenReturn(List.of(node));
    Mockito.when(cluster.clusterBean()).thenReturn(ClusterBean.of(Map.of()));
    Mockito.when(cluster.topics()).thenReturn(Set.of("t"));
    Mockito.when(cluster.availableReplicas("t"))
        .thenReturn(List.of(ReplicaInfo.of("t", 0, node, true, true, false)));

    var cost =
        throughputCost.brokerCost(ClusterInfo.of(cluster, Map.of(10, List.of(bean)))).value();
    Assertions.assertEquals(1, cost.size());
    Assertions.assertEquals(1, cost.get(10));
  }
}
