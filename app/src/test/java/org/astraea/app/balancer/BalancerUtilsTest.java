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
package org.astraea.app.balancer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.cost.Configuration;
import org.astraea.common.cost.ReplicaDiskInCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BalancerUtilsTest {
  private static final HasGauge OLD_TP1_0 =
      fakePartitionBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 1000, 1000L);
  private static final HasGauge NEW_TP1_0 =
      fakePartitionBeanObject(
          "Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 500000, 10000L);
  private static final HasGauge OLD_TP1_1 =
      fakePartitionBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "1", 500, 1000L);
  private static final HasGauge NEW_TP1_1 =
      fakePartitionBeanObject(
          "Log", LogMetrics.Log.SIZE.metricName(), "test-1", "1", 100000000, 10000L);
  private static final HasGauge LEADER_BROKER1 =
      fakeBrokerBeanObject(
          "ReplicaManager", ServerMetrics.ReplicaManager.LEADER_COUNT.metricName(), 2, 10000L);
  private static final HasGauge LEADER_BROKER2 =
      fakeBrokerBeanObject(
          "ReplicaManager", ServerMetrics.ReplicaManager.LEADER_COUNT.metricName(), 4, 10000L);
  private static final Collection<HasBeanObject> broker1 =
      List.of(OLD_TP1_0, NEW_TP1_0, LEADER_BROKER1);
  private static final Collection<HasBeanObject> broker2 =
      List.of(OLD_TP1_1, NEW_TP1_1, LEADER_BROKER2);
  private static final Map<Integer, Collection<HasBeanObject>> beanObjects =
      Map.of(0, broker1, 1, broker2);

  @Test
  void testMockClusterInfoAllocation() {
    var tp1 = TopicPartition.of("testMockCluster", 1);
    var tp2 = TopicPartition.of("testMockCluster", 0);
    var logPlacement1 = List.of(LogPlacement.of(0, "/data"), LogPlacement.of(1, "/data"));
    var logPlacement2 = List.of(LogPlacement.of(1, "/data1"), LogPlacement.of(2, "/data1"));
    var clusterInfo =
        ClusterInfo.of(
            List.of(
                NodeInfo.of(0, "localhost", 9092),
                NodeInfo.of(1, "localhost", 9092),
                NodeInfo.of(2, "localhost", 9092)),
            List.of(
                Replica.of(
                    "test-1",
                    1,
                    NodeInfo.of(0, "localhost", 9092),
                    0,
                    100,
                    true,
                    true,
                    false,
                    false,
                    true,
                    "/tmp/aa")));
    var cla = ClusterLogAllocation.of(Map.of(tp1, logPlacement1, tp2, logPlacement2));
    var mockClusterInfo = BalancerUtils.merge(clusterInfo, cla);
    Assertions.assertEquals(mockClusterInfo.replicas("testMockCluster").size(), 4);
    Assertions.assertEquals(mockClusterInfo.nodes().size(), 3);
    Assertions.assertEquals(mockClusterInfo.topics().size(), 1);
    Assertions.assertTrue(mockClusterInfo.topics().contains("testMockCluster"));
  }

  @Test
  void testEvaluateCost() {
    var node1 = NodeInfo.of(0, "localhost", 9092);
    var node2 = NodeInfo.of(1, "localhost", 9092);
    var clusterInfo =
        ClusterInfo.of(
            List.of(node1, node2),
            List.of(
                Replica.of("test-1", 0, node1, 0, 100, true, true, false, false, true, "/tmp/aa"),
                Replica.of("test-1", 1, node2, 0, 100, true, true, false, false, true, "/tmp/aa")));
    var cf1 = new ReplicaLeaderCost();
    var cf2 = new ReplicaDiskInCost(Configuration.of(Map.of("metrics.duration", "5")));
    var cost = BalancerUtils.evaluateCost(clusterInfo, Map.of(cf1, beanObjects, cf2, beanObjects));
    Assertions.assertEquals(1.3234028368582615, cost);
  }

  private static LogMetrics.Log.Gauge fakePartitionBeanObject(
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
