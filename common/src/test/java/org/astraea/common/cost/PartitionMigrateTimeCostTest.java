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
import java.util.stream.Collectors;
import org.apache.kafka.common.Node;
import org.astraea.common.Configuration;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.ClusterBean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PartitionMigrateTimeCostTest {

  private static final BeanObject inBean0 =
      new BeanObject("domain", Map.of(), Map.of("OneMinuteRate", 1000.0));
  private static final BeanObject outBean0 =
      new BeanObject("domain", Map.of(), Map.of("OneMinuteRate", 1500.0));
  private static final BeanObject inBean1 =
      new BeanObject("domain", Map.of(), Map.of("OneMinuteRate", 2000.0));
  private static final BeanObject outBean1 =
      new BeanObject("domain", Map.of(), Map.of("OneMinuteRate", 2500.0));
  private static final BeanObject inBean2 =
      new BeanObject("domain", Map.of(), Map.of("OneMinuteRate", 3000.0));
  private static final BeanObject outBean2 =
      new BeanObject("domain", Map.of(), Map.of("OneMinuteRate", 3500.0));

  @Test
  void testMigratedCost() {
    // before(partition-broker): p10-1, p11-2, p12-0, p12-0
    // after(partition-broker):  p10-0, p11-1, p12-2, p12-0
    // p10:777, p11:700, p12:500
    var before = of(before(), brokers());
    var after = of(after(), brokers());
    var migrationCost = MigrationCost.brokerMigrationTime(before, after, clusterBean());
    Assertions.assertEquals(Math.max(10000000 / 1000, 30000000 / 1500), migrationCost.get(0));
    Assertions.assertEquals(Math.max(20000000 / 2000, 10000000 / 2500), migrationCost.get(1));
    Assertions.assertEquals(Math.max(30000000 / 3000, 20000000 / 3500), migrationCost.get(2));
  }

  private List<NodeInfo> brokers() {
    return before().stream()
        .map(Replica::nodeInfo)
        .distinct()
        .map(
            nodeInfo ->
                Broker.of(
                    false,
                    new Node(nodeInfo.id(), "", nodeInfo.port()),
                    Map.of(),
                    Map.of(),
                    List.of()))
        .collect(Collectors.toList());
  }

  @Test
  void testMostCost() {
    var before = of(before(), brokers());
    var after = of(after(), brokers());
    var timeLimit =
        Configuration.of(Map.of(PartitionMigrateTimeCost.MAX_MIGRATE_TIME_KEY, "20000"));
    var overFlowTimeLimit =
        Configuration.of(Map.of(PartitionMigrateTimeCost.MAX_MIGRATE_TIME_KEY, "19999"));
    var cf = new PartitionMigrateTimeCost(timeLimit);
    var overFlowCf = new PartitionMigrateTimeCost(overFlowTimeLimit);
    var moveCost = cf.moveCost(before, after, clusterBean());
    var overflowCost = overFlowCf.moveCost(before, after, clusterBean());
    Assertions.assertFalse(moveCost.overflow());
    Assertions.assertTrue(overflowCost.overflow());
  }

  public static ClusterInfo of(List<Replica> replicas, List<NodeInfo> nodeInfos) {
    return ClusterInfo.of("fake", nodeInfos, Map.of(), replicas);
  }

  private List<Replica> after() {
    return List.of(
        Replica.builder()
            .topic("t")
            .partition(10)
            .isLeader(true)
            .size(10000000)
            .nodeInfo(NodeInfo.of(1, "", -1))
            .build(),
        Replica.builder()
            .topic("t")
            .partition(11)
            .isLeader(true)
            .size(20000000)
            .nodeInfo(NodeInfo.of(2, "", -1))
            .build(),
        Replica.builder()
            .topic("t")
            .partition(12)
            .isLeader(true)
            .size(30000000)
            .nodeInfo(NodeInfo.of(0, "", -1))
            .build(),
        Replica.builder()
            .topic("t")
            .partition(12)
            .isLeader(false)
            .size(30000000)
            .nodeInfo(NodeInfo.of(0, "", -1))
            .build());
  }

  private List<Replica> before() {
    return List.of(
        Replica.builder()
            .topic("t")
            .partition(10)
            .isLeader(true)
            .size(10000000)
            .nodeInfo(NodeInfo.of(0, "", -1))
            .build(),
        Replica.builder()
            .topic("t")
            .partition(11)
            .isLeader(true)
            .size(20000000)
            .nodeInfo(NodeInfo.of(1, "", -1))
            .build(),
        Replica.builder()
            .topic("t")
            .partition(12)
            .isLeader(true)
            .size(30000000)
            .nodeInfo(NodeInfo.of(2, "", -1))
            .build(),
        Replica.builder()
            .topic("t")
            .partition(12)
            .isLeader(false)
            .size(30000000)
            .nodeInfo(NodeInfo.of(0, "", -1))
            .build());
  }

  private static ClusterBean clusterBean() {
    return ClusterBean.of(
        Map.of(
            0,
            List.of(
                (PartitionMigrateTimeCost.MaxReplicationInRateBean) () -> inBean0,
                (PartitionMigrateTimeCost.MaxReplicationOutRateBean) () -> outBean0),
            1,
            List.of(
                (PartitionMigrateTimeCost.MaxReplicationInRateBean) () -> inBean1,
                (PartitionMigrateTimeCost.MaxReplicationOutRateBean) () -> outBean1),
            2,
            List.of(
                (PartitionMigrateTimeCost.MaxReplicationInRateBean) () -> inBean2,
                (PartitionMigrateTimeCost.MaxReplicationOutRateBean) () -> outBean2)));
  }
}
