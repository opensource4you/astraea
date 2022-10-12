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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ClusterIntegratedCostTest {
  private static final HasGauge<Long> OLD_TP1_0 =
      fakeBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 1000, 1000L);
  private static final HasGauge<Long> NEW_TP1_0 =
      fakeBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 500000, 60000L);
  private static final HasGauge<Long> OLD_TP1_1 =
      fakeBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "1", 500, 1000L);
  private static final HasGauge<Long> NEW_TP1_1 =
      fakeBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "1", 1000000, 60000L);
  private static final HasGauge<Long> OLD_TP2_0 =
      fakeBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-2", "0", 200, 1000L);
  private static final HasGauge<Long> NEW_TP2_0 =
      fakeBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-2", "0", 400000, 60000L);
  private static final HasBeanObject LeaderCount_1 =
      fakeBrokerBeanObject(
          "ReplicaManager", ServerMetrics.ReplicaManager.LEADER_COUNT.metricName(), 2, 10000L);
  private static final HasBeanObject LeaderCount_2 =
      fakeBrokerBeanObject(
          "ReplicaManager", ServerMetrics.ReplicaManager.LEADER_COUNT.metricName(), 1, 10000L);
  private static final HasBeanObject LeaderCount_3 =
      fakeBrokerBeanObject(
          "ReplicaManager", ServerMetrics.ReplicaManager.LEADER_COUNT.metricName(), 0, 10000L);

  @Test
  void testClusterCost() {
    var cost = new ClusterIntegratedCost();
    var clusterBean = ClusterBean.of(clusterBean());
    var nodeInfos =
        List.of(NodeInfo.of(1, "h", 100), NodeInfo.of(2, "h", 100), NodeInfo.of(3, "h", 100));
    var replicas =
        List.of(
            Replica.builder()
                .nodeInfo(nodeInfos.get(0))
                .topic("test-1")
                .partition(0)
                .size(500000)
                .build(),
            Replica.builder()
                .nodeInfo(nodeInfos.get(0))
                .topic("test-1")
                .partition(1)
                .size(1000000)
                .build(),
            Replica.builder()
                .nodeInfo(nodeInfos.get(1))
                .topic("test-2")
                .partition(0)
                .size(400000)
                .build());
    var clusterInfo = ClusterInfo.of(new HashSet<>(nodeInfos), replicas);
    var clusterCost = cost.clusterCost(clusterInfo, clusterBean);

    var costs =
        List.of(
            new ReplicaSizeCost(),
            new ReplicaLeaderCost(),
            new ReplicaDiskInCost(Configuration.of(Map.of())));
    var brokerCosts =
        costs.stream()
            .map(
                x ->
                    Map.entry(
                        cost.costName(x), x.brokerCost(clusterInfo, clusterBean).value().values()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var weight = WeightProvider.entropy(Normalizer.minMax(true)).weight(brokerCosts);
    var assertClusterCost =
        costs.stream()
            .map(x -> Map.entry(cost.costName(x), x.clusterCost(clusterInfo, clusterBean)))
            .mapToDouble(x -> x.getValue().value() * weight.get(x.getKey()))
            .sum();
    Assertions.assertEquals(assertClusterCost, clusterCost.value());
  }

  private static Map<Integer, Collection<HasBeanObject>> clusterBean() {
    return Map.of(
        1, List.of(OLD_TP1_0, NEW_TP1_0, OLD_TP2_0, NEW_TP2_0, LeaderCount_1),
        2, List.of(OLD_TP1_0, NEW_TP1_0, OLD_TP1_1, NEW_TP1_1, LeaderCount_2),
        3, List.of(OLD_TP1_1, NEW_TP1_1, OLD_TP2_0, NEW_TP2_0, LeaderCount_3));
  }

  private static LogMetrics.Log.Gauge fakeBeanObject(
      String type, String name, String topic, String partition, long size, long time) {
    return new LogMetrics.Log.Gauge(
        new BeanObject(
            "kafka.log",
            Map.of("name", name, "type", type, "topic", topic, "partition", partition),
            Map.of("Value", size),
            time));
  }

  private static ServerMetrics.ReplicaManager.Gauge fakeBrokerBeanObject(
      String type, String name, long value, long time) {
    return new ServerMetrics.ReplicaManager.Gauge(
        new BeanObject(
            "kafka.server", Map.of("type", type, "name", name), Map.of("Value", value), time));
  }
}
