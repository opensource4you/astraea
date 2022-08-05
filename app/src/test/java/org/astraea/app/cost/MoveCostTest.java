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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.broker.HasCount;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.broker.LogMetrics;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.partitioner.Configuration;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MoveCostTest extends RequireBrokerCluster {
  private static MoveCost moveCost;
  private static ClusterBean clusterBean;
  private static Duration duration;

  @BeforeAll
  static void initCost() throws IOException {
    var bandwidthConfigPath = "/tmp/testBrokerBandwidthProperties";
    var logSizeConfigPath = "/tmp/testBrokerSizeProperties";
    OutputStream output = new FileOutputStream(bandwidthConfigPath);
    Properties prop = new Properties();
    prop.setProperty("broker.0", "1000");
    prop.setProperty("broker.1", "1000");
    prop.setProperty("broker.2", "1000");
    prop.store(output, null);
    OutputStream output2 = new FileOutputStream(logSizeConfigPath);
    Properties prop2 = new Properties();
    prop2.store(output, null);
    prop2.setProperty("broker.0./logPath01", "1000");
    prop2.setProperty("broker.0./logPath02", "1000");
    prop2.setProperty("broker.1./logPath01", "1000");
    prop2.setProperty("broker.1./logPath02", "1000");
    prop2.setProperty("broker.2./logPath01", "1000");
    prop2.setProperty("broker.2./logPath02", "1000");
    prop2.store(output2, null);
    var configuration =
        Configuration.of(
            Map.of(
                "metrics.duration",
                "5",
                "brokerBandwidthConfig",
                bandwidthConfigPath,
                "brokerCapacityConfig",
                logSizeConfigPath));

    var fakeBeanObjectByteIn1 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics", KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), 1000, 1000);
    var fakeBeanObjectByteIn2 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics",
            KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(),
            100000000,
            10000);
    var fakeBeanObjectByteOut1 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics", KafkaMetrics.BrokerTopic.BytesOutPerSec.metricName(), 1000, 1000);
    var fakeBeanObjectByteOut2 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics",
            KafkaMetrics.BrokerTopic.BytesOutPerSec.metricName(),
            100000000,
            10000);
    var fakeBeanObjectReplicationByteIn1 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics",
            KafkaMetrics.BrokerTopic.ReplicationBytesInPerSec.metricName(),
            1000,
            1000);
    var fakeBeanObjectReplicationByteIn2 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics",
            KafkaMetrics.BrokerTopic.ReplicationBytesInPerSec.metricName(),
            100000000,
            10000);
    var fakeBeanObjectReplicationByteOut1 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics",
            KafkaMetrics.BrokerTopic.ReplicationBytesOutPerSec.metricName(),
            1000,
            1000);
    var fakeBeanObjectReplicationByteOut2 =
        fakeBrokerBeanObject(
            "BrokerTopicMetrics",
            KafkaMetrics.BrokerTopic.ReplicationBytesOutPerSec.metricName(),
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

    moveCost = new MoveCost(configuration);
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
    duration =
        Duration.ofSeconds(Integer.parseInt(configuration.string("metrics.duration").orElse("30")));
  }

  @Test
  void testBrokerTrafficMetrics() {
    var brokerTraffic =
        moveCost.brokerTrafficMetrics(
            clusterBean, KafkaMetrics.BrokerTopic.BytesInPerSec.metricName(), duration);
    Assertions.assertEquals(
        brokerTraffic.get(0), (100000000 - 1000) / 1024.0 / 1024.0 / ((10000.0 - 1000.0) / 1000));
  }

  @Test
  void testCheckBrokerInTraffic() throws IOException {
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
            new MoveCost.ReplicaMigrateInfo(
                TopicPartition.of("test-1", 0), 0, 1, "/logPath01", "/logPath02"),
            new MoveCost.ReplicaMigrateInfo(
                TopicPartition.of("test-1", 1), 0, 1, "/logPath01", "/logPath02"));
    var notOverflow = moveCost.checkBrokerInTraffic(replicaDataRate, migratedReplicas, clusterBean);
    var overflow =
        moveCost.checkBrokerInTraffic(overflowReplicaDataRate, migratedReplicas, clusterBean);
    Assertions.assertFalse(notOverflow);
    Assertions.assertTrue(overflow);
  }

  @Test
  void checkFolderSize() {
    var tprList =
        List.of(
            TopicPartitionReplica.of("test-1", 0, 0),
            TopicPartitionReplica.of("test-1", 1, 0),
            TopicPartitionReplica.of("test-1", 2, 1),
            TopicPartitionReplica.of("test-1", 3, 1));
    var replicaLogSize =
        Map.of(
            tprList.get(0),
            524288000L,
            tprList.get(1),
            524288000L,
            tprList.get(2),
            120000L,
            tprList.get(3),
            15000L);
    var migratedReplicas =
        List.of(
            new MoveCost.ReplicaMigrateInfo(
                TopicPartition.of("test-1", 0), 0, 1, "/logPath01", "/logPath01"),
            new MoveCost.ReplicaMigrateInfo(
                TopicPartition.of("test-1", 1), 0, 1, "/logPath02", "/logPath02"));
    var overflowMigratedReplicas =
        List.of(
            new MoveCost.ReplicaMigrateInfo(
                TopicPartition.of("test-1", 0), 0, 1, "/logPath01", "/logPath01"),
            new MoveCost.ReplicaMigrateInfo(
                TopicPartition.of("test-1", 1), 0, 1, "/logPath02", "/logPath01"));

    var notOverflow = moveCost.checkFolderSize(replicaLogSize, migratedReplicas);
    var overflow = moveCost.checkFolderSize(replicaLogSize, overflowMigratedReplicas);
    Assertions.assertFalse(notOverflow);
    Assertions.assertTrue(overflow);
  }

  @Test
  void testClusterCost() {
    var score = moveCost.clusterCost(originClusterInfo(), newClusterInfo(), clusterBean).value();
    Assertions.assertEquals(score, 0.2622827348460017);
  }

  private static HasValue fakePartitionBeanObject(
      String type, String name, String topic, String partition, long size, long time) {
    BeanObject beanObject =
        new BeanObject(
            "kafka.log",
            Map.of("name", name, "type", type, "topic", topic, "partition", partition),
            Map.of("Value", size),
            time);
    return HasValue.of(beanObject);
  }

  private static HasCount fakeBrokerBeanObject(String type, String name, long count, long time) {
    BeanObject beanObject =
        new BeanObject(
            "kafka.server", Map.of("type", type, "name", name), Map.of("Count", count), time);
    return HasCount.of(beanObject);
  }

  private ClusterInfo originClusterInfo() {

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

  private ClusterInfo newClusterInfo() {

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
}
