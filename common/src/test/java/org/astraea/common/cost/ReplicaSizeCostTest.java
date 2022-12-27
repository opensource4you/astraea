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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.collector.MetricCollector;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReplicaSizeCostTest extends RequireBrokerCluster {
  private static final BeanObject bean1 =
      new BeanObject(
          "domain",
          Map.of("topic", "t", "partition", "10", "name", "SIZE"),
          Map.of("Value", 777.0));
  private static final BeanObject bean2 =
      new BeanObject(
          "domain",
          Map.of("topic", "t", "partition", "11", "name", "SIZE"),
          Map.of("Value", 700.0));
  private static final BeanObject bean3 =
      new BeanObject(
          "domain",
          Map.of("topic", "t", "partition", "12", "name", "SIZE"),
          Map.of("Value", 500.0),
          200);
  private static final BeanObject bean4 =
      new BeanObject(
          "domain",
          Map.of("topic", "t", "partition", "12", "name", "SIZE"),
          Map.of("Value", 500.0),
          200);

  @Test
  void testClusterCost() {
    final Dispersion dispersion = Dispersion.cov();
    var loadCostFunction = new ReplicaSizeCost();
    var brokerLoad = loadCostFunction.brokerCost(clusterInfo(), clusterBean()).value();
    var clusterCost = loadCostFunction.clusterCost(clusterInfo(), clusterBean()).value();
    Assertions.assertEquals(dispersion.calculate(brokerLoad.values()), clusterCost);
  }

  @Test
  void testBrokerCost() {
    var cost = new ReplicaSizeCost();
    var result = cost.brokerCost(clusterInfo(), clusterBean());
    Assertions.assertEquals(3, result.value().size());
    Assertions.assertEquals(777 + 500, result.value().get(0));
    Assertions.assertEquals(700, result.value().get(1));
    Assertions.assertEquals(500, result.value().get(2));
  }

  @Test
  void testMoveCost() {
    var cost = new ReplicaSizeCost();
    var moveCost = cost.moveCost(originClusterInfo(), newClusterInfo(), ClusterBean.EMPTY);

    Assertions.assertEquals(
        3, moveCost.movedReplicaSize().size(), moveCost.movedReplicaSize().toString());
    Assertions.assertEquals(700000, moveCost.movedReplicaSize().get(0).bytes());
    Assertions.assertEquals(-6700000, moveCost.movedReplicaSize().get(1).bytes());
    Assertions.assertEquals(6000000, moveCost.movedReplicaSize().get(2).bytes());
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

  static ClusterInfo<Replica> getClusterInfo(List<Replica> replicas) {
    return ClusterInfo.of(
        replicas.stream().map(ReplicaInfo::nodeInfo).collect(Collectors.toList()), replicas);
  }

  static ClusterInfo<Replica> originClusterInfo() {
    var replicas =
        List.of(
            Replica.builder()
                .topic("test-1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .lag(-1)
                .size(6000000)
                .isLeader(true)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .lag(-1)
                .size(6000000)
                .isLeader(false)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(1)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .lag(-1)
                .size(700000)
                .isLeader(true)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .lag(-1)
                .size(700000)
                .isLeader(false)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-2")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .lag(-1)
                .size(800000)
                .isLeader(true)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/log-path-01")
                .build(),
            Replica.builder()
                .topic("test-2")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .lag(-1)
                .size(800000)
                .isLeader(false)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/log-path-02")
                .build());
    return getClusterInfo(replicas);
  }

  static ClusterInfo<Replica> newClusterInfo() {
    var replicas =
        List.of(
            Replica.builder()
                .topic("test-1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .lag(-1)
                .size(6000000)
                .isLeader(true)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .lag(-1)
                .size(6000000)
                .isLeader(false)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .lag(-1)
                .size(700000)
                .isLeader(true)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .lag(-1)
                .size(700000)
                .isLeader(false)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("test-2")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .lag(-1)
                .size(800000)
                .isLeader(true)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/log-path-01")
                .build(),
            Replica.builder()
                .topic("test-2")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .lag(-1)
                .size(800000)
                .isLeader(false)
                .inSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("/log-path-03")
                .build());
    return getClusterInfo(replicas);
  }

  @Test
  void testPartitionCost() {
    var cost = new ReplicaSizeCost();
    var result = cost.partitionCost(clusterInfo(), clusterBean()).value();

    Assertions.assertEquals(3, result.size());
    Assertions.assertEquals(777.0, result.get(TopicPartition.of("t", 10)));
    Assertions.assertEquals(700.0, result.get(TopicPartition.of("t", 11)));
    Assertions.assertEquals(500.0, result.get(TopicPartition.of("t", 12)));
  }

  private ClusterInfo<Replica> clusterInfo() {
    var replicas =
        List.of(
            Replica.builder()
                .topic("t")
                .partition(10)
                .isLeader(true)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .build(),
            Replica.builder()
                .topic("t")
                .partition(11)
                .isLeader(true)
                .nodeInfo(NodeInfo.of(1, "", -1))
                .build(),
            Replica.builder()
                .topic("t")
                .partition(12)
                .isLeader(true)
                .nodeInfo(NodeInfo.of(2, "", -1))
                .build(),
            Replica.builder()
                .topic("t")
                .partition(12)
                .isLeader(false)
                .nodeInfo(NodeInfo.of(0, "", -1))
                .build());
    return ClusterInfo.of(
        List.of(NodeInfo.of(0, "", -1), NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1)), replicas);
  }

  private static ClusterBean clusterBean() {

    return ClusterBean.of(
        Map.of(
            0,
            List.of(
                (ReplicaSizeCost.SizeStatisticalBean) () -> bean1,
                (ReplicaSizeCost.SizeStatisticalBean) () -> bean4),
            1,
            List.of((ReplicaSizeCost.SizeStatisticalBean) () -> bean2),
            2,
            List.of((ReplicaSizeCost.SizeStatisticalBean) () -> bean3)));
  }

  @Test
  void testMaxPartitionSize() {
    var cost = new ReplicaSizeCost();
    var tp = TopicPartition.of("t", 0);
    var overFlowSize = List.of(100.0, 200.0, 300.0);
    var size = List.of(100.0, 110.0, 120.0);
    Assertions.assertThrows(
        NoSufficientMetricsException.class, () -> cost.maxPartitionSize(tp, List.of()));
    Assertions.assertThrows(
        NoSufficientMetricsException.class, () -> cost.maxPartitionSize(tp, overFlowSize));
    Assertions.assertEquals(120.0, cost.maxPartitionSize(tp, size));
  }

  @Test
  void testFetcher() throws InterruptedException {
    var interval = Duration.ofMillis(300);
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      try (var collector = MetricCollector.builder().interval(interval).build()) {
        var costFunction = new ReplicaSizeCost();
        // when cluster has partitions,each partition will correspond to a statistical bean
        Assertions.assertThrows(
            NoSufficientMetricsException.class,
            () -> costFunction.clusterCost(clusterInfo(), ClusterBean.EMPTY));
        Assertions.assertDoesNotThrow(
            () -> costFunction.clusterCost(ClusterInfo.empty(), ClusterBean.EMPTY));

        // create come partition to get metric
        admin
            .creator()
            .topic(topicName)
            .numberOfPartitions(4)
            .numberOfReplicas((short) 1)
            .run()
            .toCompletableFuture()
            .get();
        var producer = Producer.of(bootstrapServers());
        producer
            .send(Record.builder().topic(topicName).partition(0).key(new byte[100]).build())
            .toCompletableFuture()
            .join();
        collector.addFetcher(
            costFunction.fetcher().orElseThrow(), (id, err) -> Assertions.fail(err.getMessage()));
        collector.registerLocalJmx(0);
        costFunction.sensors().forEach(collector::addMetricSensors);
        var tpr =
            List.of(
                TopicPartitionReplica.of(topicName, 0, 0),
                TopicPartitionReplica.of(topicName, 1, 0),
                TopicPartitionReplica.of(topicName, 2, 0),
                TopicPartitionReplica.of(topicName, 3, 0));
        Utils.sleep(interval);
        Assertions.assertEquals(
            170 * 0.5,
            collector
                .clusterBean()
                .replicaMetrics(tpr.get(0), ReplicaSizeCost.SizeStatisticalBean.class)
                .findFirst()
                .orElseThrow()
                .value());
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
