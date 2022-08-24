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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.broker.HasCount;
import org.astraea.app.metrics.broker.LogMetrics;
import org.astraea.app.metrics.broker.ServerMetrics;
import org.astraea.app.partitioner.Configuration;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ResourceCheckCostTest extends RequireBrokerCluster {
  private static ResourceCheckCost resourceCheckCost;
  private static ClusterBean clusterBean;
  private static Duration duration;

  @BeforeAll
  static void initCost() {
    var configuration = Configuration.of(Map.of("metrics.duration", "5"));
    var fakeBeanObjectByteIn1 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics", ServerMetrics.Topic.BYTES_IN_PER_SEC.metricName(), 1000, 1000);
    var fakeBeanObjectByteIn2 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics",
            ServerMetrics.Topic.BYTES_IN_PER_SEC.metricName(),
            100000000,
            10000);
    var fakeBeanObjectByteOut1 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics", ServerMetrics.Topic.BYTES_OUT_PER_SEC.metricName(), 1000, 1000);
    var fakeBeanObjectByteOut2 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics",
            ServerMetrics.Topic.BYTES_OUT_PER_SEC.metricName(),
            100000000,
            10000);
    var fakeBeanObjectReplicationByteIn1 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics",
            ServerMetrics.Topic.REPLICATION_BYTES_IN_PER_SEC.metricName(),
            1000,
            1000);
    var fakeBeanObjectReplicationByteIn2 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics",
            ServerMetrics.Topic.REPLICATION_BYTES_IN_PER_SEC.metricName(),
            100000000,
            10000);
    var fakeBeanObjectReplicationByteOut1 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics",
            ServerMetrics.Topic.REPLICATION_BYTES_OUT_PER_SEC.metricName(),
            1000,
            1000);
    var fakeBeanObjectReplicationByteOut2 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics",
            ServerMetrics.Topic.REPLICATION_BYTES_OUT_PER_SEC.metricName(),
            100000000,
            10000);
    var replicaSizeBeanObject1 =
        fakePartitionBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 100, 1000);
    var replicaSizeBeanObject2 =
        fakePartitionBeanObject(
            "Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 6000000, 10000);
    var replicaSizeBeanObject3 =
        fakePartitionBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "1", 100, 1000);
    var replicaSizeBeanObject4 =
        fakePartitionBeanObject(
            "Log", LogMetrics.Log.SIZE.metricName(), "test-1", "1", 700000, 10000);
    var replicaSizeBeanObject5 =
        fakePartitionBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-2", "0", 100, 1000);
    var replicaSizeBeanObject6 =
        fakePartitionBeanObject(
            "Log", LogMetrics.Log.SIZE.metricName(), "test-2", "0", 800000, 10000);
    clusterBean =
        ClusterBean.of(
            Map.of(
                0,
                List.of(
                    fakeBeanObjectByteIn1,
                    fakeBeanObjectByteIn2,
                    fakeBeanObjectByteOut1,
                    fakeBeanObjectByteOut2,
                    fakeBeanObjectReplicationByteIn1,
                    fakeBeanObjectReplicationByteIn2,
                    fakeBeanObjectReplicationByteOut1,
                    fakeBeanObjectReplicationByteOut2,
                    replicaSizeBeanObject1,
                    replicaSizeBeanObject2,
                    replicaSizeBeanObject5,
                    replicaSizeBeanObject6),
                1,
                List.of(
                    fakeBeanObjectByteIn1,
                    fakeBeanObjectByteIn2,
                    fakeBeanObjectByteOut1,
                    fakeBeanObjectByteOut2,
                    fakeBeanObjectReplicationByteIn1,
                    fakeBeanObjectReplicationByteIn2,
                    fakeBeanObjectReplicationByteOut1,
                    fakeBeanObjectReplicationByteOut2,
                    replicaSizeBeanObject1,
                    replicaSizeBeanObject2,
                    replicaSizeBeanObject3,
                    replicaSizeBeanObject4),
                2,
                List.of(
                    fakeBeanObjectByteIn1,
                    fakeBeanObjectByteIn2,
                    fakeBeanObjectByteOut1,
                    fakeBeanObjectByteOut2,
                    fakeBeanObjectReplicationByteIn1,
                    fakeBeanObjectReplicationByteIn2,
                    fakeBeanObjectReplicationByteOut1,
                    fakeBeanObjectReplicationByteOut2,
                    replicaSizeBeanObject3,
                    replicaSizeBeanObject4,
                    replicaSizeBeanObject5,
                    replicaSizeBeanObject6)));
    resourceCheckCost = new ResourceCheckCost(configuration, clusterBean);
    duration =
        Duration.ofSeconds(Integer.parseInt(configuration.string("metrics.duration").orElse("30")));
  }

  @Test
  void testBrokerTrafficMetrics() {
    var brokerTraffic =
        ResourceCheckCost.brokerTrafficMetrics(
            clusterBean, ServerMetrics.Topic.BYTES_OUT_PER_SEC.metricName(), duration);
    Assertions.assertEquals(
        brokerTraffic.get(0), (100000000 - 1000) / 1024.0 / 1024.0 / ((10000.0 - 1000.0) / 1000));
  }

  @Test
  void testCheckBrokerInTraffic() {
    var tprList =
        List.of(
            TopicPartitionReplica.of("test-1", 0, 0),
            TopicPartitionReplica.of("test-1", 1, 0),
            TopicPartitionReplica.of("test-1", 2, 1),
            TopicPartitionReplica.of("test-1", 3, 1),
            TopicPartitionReplica.of("test-1", 4, 2),
            TopicPartitionReplica.of("test-1", 5, 2));
    var replicaDataRate =
        Map.of(
            tprList.get(0),
            10.0,
            tprList.get(1),
            5.0,
            tprList.get(2),
            8.0,
            tprList.get(3),
            15.0,
            tprList.get(4),
            10.0,
            tprList.get(5),
            11.0);
    var overflowReplicaDataRate =
        Map.of(
            tprList.get(0),
            1000000000000.0,
            tprList.get(1),
            5.0,
            tprList.get(2),
            8.0,
            tprList.get(3),
            15.0,
            tprList.get(4),
            10.0,
            tprList.get(5),
            12.0);
    var migratedReplicas =
        List.of(
            new ResourceCheckCost.MigrateInfo(
                TopicPartition.of("test-1", 0), 0, 1, "/logPath01", "/logPath02"),
            new ResourceCheckCost.MigrateInfo(
                TopicPartition.of("test-1", 1), 0, 1, "/logPath01", "/logPath02"));
    var notOverflow =
        resourceCheckCost.checkBrokerInTraffic(replicaDataRate, migratedReplicas, clusterBean);
    var overflow =
        resourceCheckCost.checkBrokerInTraffic(
            overflowReplicaDataRate, migratedReplicas, clusterBean);
    Assertions.assertFalse(notOverflow);
    Assertions.assertTrue(overflow);
  }

  @Test
  void testClusterCost() {
    var score =
        resourceCheckCost.moveCost(originClusterInfo(), newClusterInfo(), clusterBean).value();
    Assertions.assertEquals(0.99012377645703, score);
  }

  static ClusterInfo originClusterInfo() {

    ClusterInfo clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.nodes())
        .thenReturn(
            List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1)));
    Mockito.when(clusterInfo.topics()).thenReturn(Set.of("test-1", "test-2"));
    Mockito.when(clusterInfo.availableReplicaLeaders(Mockito.anyString()))
        .thenAnswer(
            topic ->
                topic.getArgument(0).equals("test-1")
                    ? List.of(
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(0, "", -1), true, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(2, "", -1), true, true, false))
                    : List.of(
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(2, "", -1), true, true, false)));
    Mockito.when(clusterInfo.replicas(Mockito.anyString()))
        .thenAnswer(
            topic ->
                topic.getArgument(0).equals("test-1")
                    ? List.of(
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(0, "", -1), true, true, false),
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(1, "", -1), false, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(1, "", -1), false, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(2, "", -1), true, true, false))
                    : List.of(
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(0, "", -1), false, true, false),
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(2, "", -1), true, true, false)));
    Mockito.when(clusterInfo.availableReplicas(Mockito.anyString()))
        .thenAnswer(
            topic ->
                topic.getArgument(0).equals("test-1")
                    ? List.of(
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(0, "", -1), true, true, false),
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(1, "", -1), false, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(1, "", -1), false, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(2, "", -1), true, true, false))
                    : List.of(
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(0, "", -1), false, true, false),
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(2, "", -1), true, true, false)));
    return clusterInfo;
  }

  static ClusterInfo newClusterInfo() {

    ClusterInfo clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.nodes())
        .thenReturn(
            List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1)));
    Mockito.when(clusterInfo.topics()).thenReturn(Set.of("test-1", "test-2"));
    Mockito.when(clusterInfo.availableReplicaLeaders(Mockito.anyString()))
        .thenAnswer(
            topic ->
                topic.getArgument(0).equals("test-1")
                    ? List.of(
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(2, "", -1), true, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(2, "", -1), true, true, false))
                    : List.of(
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(2, "", -1), true, true, false)));
    Mockito.when(clusterInfo.replicas(Mockito.anyString()))
        .thenAnswer(
            topic ->
                topic.getArgument(0).equals("test-1")
                    ? List.of(
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(0, "", -1), false, true, false),
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(2, "", -1), true, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(0, "", -1), false, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(2, "", -1), true, true, false))
                    : List.of(
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(1, "", -1), false, true, false),
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(2, "", -1), true, true, false)));
    Mockito.when(clusterInfo.availableReplicas(Mockito.anyString()))
        .thenAnswer(
            topic ->
                topic.getArgument(0).equals("test-1")
                    ? List.of(
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(0, "", -1), false, true, false),
                        ReplicaInfo.of("test-1", 0, NodeInfo.of(2, "", -1), true, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(0, "", -1), false, true, false),
                        ReplicaInfo.of("test-1", 1, NodeInfo.of(2, "", -1), true, true, false))
                    : List.of(
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(1, "", -1), false, true, false),
                        ReplicaInfo.of("test-2", 0, NodeInfo.of(2, "", -1), true, true, false)));
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

  static HasCount fakeBrokerBeanObject(String type, String name, long count, long time) {
    return new ServerMetrics.Topic.Meter(
        new BeanObject(
            "kafka.server", Map.of("type", type, "name", name), Map.of("Count", count), time));
  }
}
