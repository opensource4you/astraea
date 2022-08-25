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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.broker.HasGauge;
import org.astraea.app.metrics.broker.LogMetrics;
import org.astraea.app.partitioner.Configuration;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ReplicaDiskInCostTest extends RequireBrokerCluster {
  private static final HasGauge OLD_TP1_0 =
      fakeBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 1000, 1000L);
  private static final HasGauge NEW_TP1_0 =
      fakeBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 50000000, 5000L);
  private static final HasGauge OLD_TP1_1 =
      fakeBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "1", 500, 1000L);
  private static final HasGauge NEW_TP1_1 =
      fakeBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "1", 100000000, 5000L);
  private static final HasGauge OLD_TP2_0 =
      fakeBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-2", "0", 200, 1000L);
  private static final HasGauge NEW_TP2_0 =
      fakeBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-2", "0", 40000000, 5000L);

  /*
  test replica distribution :
      broker1 : test-1-0,test-2-0
      broker2 : test-1-0,test-1-1
      broker1 : test-1-1,test-2-0
   */
  private static final Collection<HasBeanObject> broker1 =
      List.of(OLD_TP1_0, NEW_TP1_0, OLD_TP2_0, NEW_TP2_0);
  private static final Collection<HasBeanObject> broker2 =
      List.of(OLD_TP1_0, NEW_TP1_0, OLD_TP1_1, NEW_TP1_1);
  private static final Collection<HasBeanObject> broker3 =
      List.of(OLD_TP1_1, NEW_TP1_1, OLD_TP2_0, NEW_TP2_0);

  @Test
  void testBrokerCost() {
    var configuration = Configuration.of(Map.of("metrics.duration", "3"));
    var loadCostFunction = new ReplicaDiskInCost(configuration);
    var brokerLoad = loadCostFunction.brokerCost(clusterInfo(), clusterBean()).value();
    Assertions.assertEquals(21.457386016845703, brokerLoad.get(1));
    Assertions.assertEquals(35.76242923736572, brokerLoad.get(2));
    Assertions.assertEquals(33.37843418121338, brokerLoad.get(3));
  }

  @Test
  void testClusterCost() {
    var configuration = Configuration.of(Map.of("metrics.duration", "3"));
    var loadCostFunction = new ReplicaDiskInCost(configuration);
    var brokerLoad = loadCostFunction.clusterCost(clusterInfo(), clusterBean()).value();
    Assertions.assertEquals(0.20721255412897746, brokerLoad);
  }

  private ClusterInfo clusterInfo() {
    ClusterInfo clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.nodes())
        .thenReturn(
            List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1)));
    Mockito.when(clusterInfo.topics()).thenReturn(Set.of("test-1", "test-2"));
    Mockito.when(clusterInfo.replicas(Mockito.anyString()))
        .thenAnswer(
            topic ->
                topic.getArgument(0).equals("test-1")
                    ? List.of(
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(1, "", -1), true, true, false),
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(2, "", -1), false, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(2, "", -1), false, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(3, "", -1), true, true, false))
                    : List.of(
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(1, "", -1), false, true, false),
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(3, "", -1), true, true, false)));
    return clusterInfo;
  }

  private static ClusterBean clusterBean() {
    return ClusterBean.of(Map.of(1, broker1, 2, broker2, 3, broker3));
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
}
