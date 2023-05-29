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

import java.util.List;
import java.util.Map;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReplicaLeaderSizeCostTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testClusterCost() {
    final Dispersion dispersion = Dispersion.standardDeviation();
    var loadCostFunction = new ReplicaLeaderSizeCost();
    var brokerLoad = loadCostFunction.brokerCost(clusterInfo(), ClusterBean.EMPTY).value();
    var clusterCost = loadCostFunction.clusterCost(clusterInfo(), ClusterBean.EMPTY).value();
    Assertions.assertEquals(dispersion.calculate(brokerLoad.values()), clusterCost);
  }

  @Test
  void testBrokerCost() {
    var cost = new ReplicaLeaderSizeCost();
    var result = cost.brokerCost(clusterInfo(), ClusterBean.EMPTY);
    Assertions.assertEquals(3, result.value().size());
    Assertions.assertEquals(777 + 500, result.value().get(0));
    Assertions.assertEquals(700, result.value().get(1));
    Assertions.assertEquals(500, result.value().get(2));
  }

  @Test
  void testPartitionCost() {
    var cost = new ReplicaLeaderSizeCost();
    var result = cost.partitionCost(clusterInfo(), ClusterBean.EMPTY).value();

    Assertions.assertEquals(3, result.size());
    Assertions.assertEquals(777.0, result.get(TopicPartition.of("t", 10)));
    Assertions.assertEquals(700.0, result.get(TopicPartition.of("t", 11)));
    Assertions.assertEquals(500.0, result.get(TopicPartition.of("t", 12)));
  }

  private ClusterInfo clusterInfo() {
    var replicas =
        List.of(
            Replica.builder().topic("t").partition(10).isLeader(true).brokerId(0).size(777).build(),
            Replica.builder().topic("t").partition(11).isLeader(true).brokerId(1).size(700).build(),
            Replica.builder().topic("t").partition(12).isLeader(true).brokerId(2).size(500).build(),
            Replica.builder()
                .topic("t")
                .partition(12)
                .isLeader(false)
                .brokerId(0)
                .size(499)
                .build());
    return ClusterInfo.of(
        "fake",
        List.of(Broker.of(0, "", -1), Broker.of(1, "", -1), Broker.of(2, "", -1)),
        Map.of(),
        replicas);
  }
}
