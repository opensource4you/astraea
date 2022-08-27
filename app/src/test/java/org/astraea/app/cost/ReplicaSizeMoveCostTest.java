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
import org.astraea.app.admin.Replica;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.broker.LogMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ReplicaSizeMoveCostTest {
  private static ClusterBean clusterBean;

  @Test
  void testMoveCost() {
    var cost = new ReplicaSizeMoveCost();
    var moveCost = cost.moveCost(originClusterInfo(), newClusterInfo(), clusterBean);
    var totalSize = moveCost.totalCost();
    var changes = moveCost.changes();

    Assertions.assertEquals(6000000 + 700000 + 800000, totalSize);
    Assertions.assertEquals(-100000, changes.get(0));
    Assertions.assertEquals(-5900000, changes.get(1));
    Assertions.assertEquals(6000000, changes.get(2));
  }

  @BeforeAll
  static void initCost() {
    var replicaSizeBeanObject1 =
        fakePartitionBeanObject(
            "Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 6000000, 10000);
    var replicaSizeBeanObject2 =
        fakePartitionBeanObject(
            "Log", LogMetrics.Log.SIZE.metricName(), "test-1", "1", 700000, 10000);
    var replicaSizeBeanObject3 =
        fakePartitionBeanObject(
            "Log", LogMetrics.Log.SIZE.metricName(), "test-2", "0", 800000, 10000);
    clusterBean =
        ClusterBean.of(
            Map.of(
                0,
                List.of(replicaSizeBeanObject1, replicaSizeBeanObject3),
                1,
                List.of(replicaSizeBeanObject1, replicaSizeBeanObject2),
                2,
                List.of(replicaSizeBeanObject2, replicaSizeBeanObject3)));
  }

  /*
  origin replica distributed :
    test-1-0 : 0,1
    test-1-1 : 1,2
    test-2-0 : 0,2

  generated plan replica distributed :
    test-1-0 : 0,2
    test-1-1 : 0,2
    test-2-0 : 1,2
   */
  static ClusterInfo originClusterInfo() {

    ClusterInfo clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.nodes())
        .thenReturn(
            List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1)));
    Mockito.when(clusterInfo.topics()).thenReturn(Set.of("test-1", "test-2"));
    Mockito.when(clusterInfo.availableReplicas(Mockito.anyString()))
        .thenAnswer(
            topic ->
                topic.getArgument(0).equals("test-1")
                    ? List.of(
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
                            true,
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
                            true,
                            true,
                            false,
                            false,
                            false,
                            ""))
                    : List.of(
                        Replica.of(
                            "test-2",
                            0,
                            NodeInfo.of(0, "", -1),
                            -1,
                            800000,
                            true,
                            true,
                            false,
                            false,
                            false,
                            ""),
                        Replica.of(
                            "test-2",
                            0,
                            NodeInfo.of(2, "", -1),
                            -1,
                            800000,
                            true,
                            true,
                            false,
                            false,
                            false,
                            "")));
    return clusterInfo;
  }

  static ClusterInfo newClusterInfo() {

    ClusterInfo clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.nodes())
        .thenReturn(
            List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1)));
    Mockito.when(clusterInfo.topics()).thenReturn(Set.of("test-1", "test-2"));
    Mockito.when(clusterInfo.availableReplicas(Mockito.anyString()))
        .thenAnswer(
            topic ->
                topic.getArgument(0).equals("test-1")
                    ? List.of(
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
                            true,
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
                            true,
                            true,
                            false,
                            false,
                            false,
                            ""))
                    : List.of(
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
                            ""),
                        Replica.of(
                            "test-2",
                            0,
                            NodeInfo.of(2, "", -1),
                            -1,
                            800000,
                            true,
                            true,
                            false,
                            false,
                            false,
                            "")));
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
