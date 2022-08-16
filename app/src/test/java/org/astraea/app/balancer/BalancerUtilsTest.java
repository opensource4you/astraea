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
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.cost.ReplicaDiskInCost;
import org.astraea.app.cost.ReplicaLeaderCost;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.broker.LogMetrics;
import org.astraea.app.metrics.broker.ServerMetrics;
import org.astraea.app.partitioner.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BalancerUtilsTest {
  private static final HasValue OLD_TP1_0 =
      fakePartitionBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 1000, 1000L);
  private static final HasValue NEW_TP1_0 =
      fakePartitionBeanObject(
          "Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 500000, 10000L);
  private static final HasValue OLD_TP1_1 =
      fakePartitionBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "1", 500, 1000L);
  private static final HasValue NEW_TP1_1 =
      fakePartitionBeanObject(
          "Log", LogMetrics.Log.SIZE.metricName(), "test-1", "1", 100000000, 10000L);
  private static final HasValue LEADER_BROKER1 =
      fakeBrokerBeanObject(
          "ReplicaManager", ServerMetrics.ReplicaManager.LEADER_COUNT.metricName(), 2, 10000L);
  private static final HasValue LEADER_BROKER2 =
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
    var logPlacement1 = List.of(LogPlacement.of(0), LogPlacement.of(1));
    var logPlacement2 = List.of(LogPlacement.of(1), LogPlacement.of(2));
    var nodes =
        new Node[] {
          new Node(0, "localhost", 9092),
          new Node(1, "localhost", 9092),
          new Node(2, "localhost", 9092)
        };
    var partitionInfo = new PartitionInfo("test-1", 1, nodes[0], nodes, nodes);
    var clusterInfo =
        ClusterInfo.of(
            new Cluster(
                "",
                List.of(nodes[0], nodes[1], nodes[2]),
                List.of(partitionInfo),
                Set.of(),
                Set.of(),
                Node.noNode()));
    var cla = ClusterLogAllocation.of(Map.of(tp1, logPlacement1, tp2, logPlacement2));
    var mockClusterInfo = BalancerUtils.merge(clusterInfo, cla);
    Assertions.assertEquals(mockClusterInfo.replicas("testMockCluster").size(), 4);
    Assertions.assertEquals(mockClusterInfo.nodes().size(), 3);
    Assertions.assertEquals(mockClusterInfo.topics().size(), 1);
    Assertions.assertTrue(mockClusterInfo.topics().contains("testMockCluster"));
  }

  @Test
  void testEvaluateCost() {
    var node1 = new Node(0, "localhost", 9092);
    var node2 = new Node(1, "localhost", 9092);
    var partitionInfo1 =
        new PartitionInfo("test-1", 0, node1, new Node[] {node1}, new Node[] {node1});
    var partitionInfo2 =
        new PartitionInfo("test-1", 1, node2, new Node[] {node2}, new Node[] {node2});
    var clusterInfo =
        ClusterInfo.of(
            new Cluster(
                "",
                List.of(node1, node2),
                List.of(partitionInfo1, partitionInfo2),
                Set.of(),
                Set.of(),
                Node.noNode()));

    var cf1 = new ReplicaLeaderCost();
    var cf2 = new ReplicaDiskInCost(Configuration.of(Map.of("metrics.duration", "5")));
    var cost = BalancerUtils.evaluateCost(clusterInfo, Map.of(cf1, beanObjects, cf2, beanObjects));
    Assertions.assertEquals(1.3234028368582615, cost);
  }

  private static LogMetrics.Log.Meter fakePartitionBeanObject(
      String type, String name, String topic, String partition, long size, long time) {
    return new LogMetrics.Log.Meter(
        new BeanObject(
            "kafka.log",
            Map.of("name", name, "type", type, "topic", topic, "partition", partition),
            Map.of("Value", size),
            time));
  }

  private static ServerMetrics.ReplicaManager.Meter fakeBrokerBeanObject(
      String type, String name, long value, long time) {
    return new ServerMetrics.ReplicaManager.Meter(
        new BeanObject(
            "kafka.server", Map.of("type", type, "name", name), Map.of("Value", value), time));
  }
}
