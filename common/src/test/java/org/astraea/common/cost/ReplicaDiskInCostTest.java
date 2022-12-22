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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.collector.MetricCollector;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReplicaDiskInCostTest extends RequireBrokerCluster {
  private static final BeanObject bean1 =
      new BeanObject("domain", Map.of("topic", "t", "partition", "10"), Map.of("Value", 777.0));
  private static final BeanObject bean2 =
      new BeanObject("domain", Map.of("topic", "t", "partition", "11"), Map.of("Value", 700.0));
  private static final BeanObject bean3 =
      new BeanObject(
          "domain", Map.of("topic", "t", "partition", "12"), Map.of("Value", 500.0), 200);
  private static final BeanObject bean4 =
      new BeanObject(
          "domain", Map.of("topic", "t", "partition", "12"), Map.of("Value", 500.0), 200);

  @Test
  void testBrokerCost() {
    var loadCostFunction = new ReplicaDiskInCost();
    var brokerLoad = loadCostFunction.brokerCost(clusterInfo(), clusterBean()).value();
    Assertions.assertEquals(777 + 500, brokerLoad.get(0));
    Assertions.assertEquals(700, brokerLoad.get(1));
    Assertions.assertEquals(500, brokerLoad.get(2));
  }

  @Test
  void testClusterCost() {
    final Dispersion dispersion = Dispersion.cov();
    var loadCostFunction = new ReplicaDiskInCost();
    var brokerLoad = loadCostFunction.brokerCost(clusterInfo(), clusterBean()).value();
    var clusterCost = loadCostFunction.clusterCost(clusterInfo(), clusterBean()).value();
    Assertions.assertEquals(dispersion.calculate(brokerLoad.values()), clusterCost);
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
                (ReplicaDiskInCost.LogRateStatisticalBean) () -> bean1,
                (ReplicaDiskInCost.LogRateStatisticalBean) () -> bean4),
            1,
            List.of((ReplicaDiskInCost.LogRateStatisticalBean) () -> bean2),
            2,
            List.of((ReplicaDiskInCost.LogRateStatisticalBean) () -> bean3)));
  }

  @Test
  void testFetcher() throws InterruptedException {
    var interval = Duration.ofSeconds(1);
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      try (var collector = MetricCollector.builder().interval(interval).build()) {
        var costFunction = new ReplicaDiskInCost();
        // create come partition to get metrics
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
        producer
            .send(Record.builder().topic(topicName).partition(0).key(new byte[100]).build())
            .toCompletableFuture()
            .join();
        Utils.sleep(interval);
        Assertions.assertEquals(
            (340 - 170) / 2,
            collector
                .clusterBean()
                .replicaMetrics(tpr.get(0), ReplicaDiskInCost.LogRateStatisticalBean.class)
                .max(Comparator.comparing(HasBeanObject::createdTimestamp))
                .orElseThrow()
                .value());
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
