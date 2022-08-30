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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.broker.LogMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ReplicaSizeCostTest {
  private static final ClusterBean clusterBean = ClusterBean.EMPTY;
  private final BeanObject bean =
      new BeanObject(
          "domain", Map.of("topic", "t", "partition", "10", "name", "SIZE"), Map.of("Value", 777));

  @Test
  void testBrokerCost() {
    var meter = new LogMetrics.Log.Gauge(bean);
    var cost = new ReplicaSizeCost();
    var result =
        cost.brokerCost(mock(ClusterInfo.class), ClusterBean.of(Map.of(1, List.of(meter))));
    Assertions.assertEquals(1, result.value().size());
    Assertions.assertEquals(777, result.value().entrySet().iterator().next().getValue());
  }

  @Test
  void testMoveCost() {
    var cost = new ReplicaSizeCost();
    var moveCost = cost.moveCost(originClusterInfo(), newClusterInfo(), clusterBean);
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
    ClusterInfo<Replica> clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.nodes())
        .thenReturn(
            List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1)));
    Mockito.when(clusterInfo.topics()).thenReturn(Set.of("test-1", "test-2"));
    Mockito.when(clusterInfo.replicas()).thenReturn(replicas);
    Mockito.when(clusterInfo.replicas(Mockito.anyString()))
        .thenAnswer(
            topic ->
                topic.getArgument(0).equals("test-1")
                    ? List.of(replicas.get(0), replicas.get(1), replicas.get(2), replicas.get(3))
                    : List.of(replicas.get(4), replicas.get(5)));
    Mockito.when(clusterInfo.replica(Mockito.isA(TopicPartitionReplica.class)))
        .thenAnswer(
            tpr ->
                clusterInfo.replicas().stream()
                    .filter(r -> r.topicPartitionReplica().equals(tpr.getArgument(0)))
                    .findFirst());
    return clusterInfo;
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
    ClusterInfo<Replica> clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.nodes())
        .thenReturn(
            List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1)));
    Mockito.when(clusterInfo.topics()).thenReturn(Set.of("test-1", "test-2"));
    Mockito.when(clusterInfo.replicas()).thenReturn(replicas);

    Mockito.when(clusterInfo.replicas()).thenReturn(replicas);
    Mockito.when(clusterInfo.replicas(Mockito.anyString()))
        .thenAnswer(
            topic ->
                topic.getArgument(0).equals("test-1")
                    ? List.of(replicas.get(0), replicas.get(1), replicas.get(2), replicas.get(3))
                    : List.of(replicas.get(4), replicas.get(5)));
    Mockito.when(clusterInfo.replica(Mockito.isA(TopicPartitionReplica.class)))
        .thenAnswer(
            tpr ->
                clusterInfo.replicas().stream()
                    .filter(r -> r.topicPartitionReplica().equals(tpr.getArgument(0)))
                    .findFirst());
    return clusterInfo;
  }

  static LogMetrics.Log.Gauge fakePartitionBeanObject(
      String type, String name, String topic, String partition, long size, long time) {
    return new LogMetrics.Log.Gauge(
        new BeanObject(
            "kafka.log",
            Map.of("name", name, "type", type, "topic", topic, "partition", partition),
            Map.of("Value", size),
            time));
  }
}
