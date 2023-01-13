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
package org.astraea.common.partitioner;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Lazy;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoTest;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.cost.BrokerCost;
import org.astraea.common.cost.BrokerInputCost;
import org.astraea.common.cost.HasBrokerCost;
import org.astraea.common.cost.NodeThroughputCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.collector.Fetcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StrictCostDispatcherTest {

  @Test
  void testJmxPort() {
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(Configuration.of(Map.of()));
      Assertions.assertEquals(Optional.empty(), dispatcher.jmxPortGetter.apply(0));
      dispatcher.configure(Configuration.of(Map.of(StrictCostDispatcher.JMX_PORT, "12345")));
      Assertions.assertEquals(Optional.of(12345), dispatcher.jmxPortGetter.apply(0));
    }
  }

  @Test
  void testNegativeWeight() {
    try (var dispatcher = new StrictCostDispatcher()) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              dispatcher.configure(
                  Configuration.of(Map.of(ReplicaLeaderCost.class.getName(), "-1"))));

      // Test for cost functions configuring
      dispatcher.configure(
          Configuration.of(
              Map.of(
                  ReplicaLeaderCost.class.getName(),
                  "0.1",
                  BrokerInputCost.class.getName(),
                  "2",
                  "jmx.port",
                  "1111")));
      Assertions.assertNotEquals(HasBrokerCost.EMPTY, dispatcher.costFunction);
    }
  }

  @Test
  void testConfigureCostFunctions() {
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(
          Configuration.of(
              Map.of(
                  ReplicaLeaderCost.class.getName(),
                  "0.1",
                  BrokerInputCost.class.getName(),
                  "2",
                  "jmx.port",
                  "1111")));
      Assertions.assertNotEquals(HasBrokerCost.EMPTY, dispatcher.costFunction);
    }
  }

  @Test
  void testNoAvailableBrokers() {
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(Configuration.EMPTY);
      Assertions.assertEquals(
          0, dispatcher.partition("topic", new byte[0], new byte[0], ClusterInfo.empty()));
    }
  }

  @Test
  void testSingleBroker() {
    var nodeInfo = NodeInfo.of(10, "host", 11111);
    var replicaInfo =
        Replica.builder()
            .topic("topic")
            .partition(10)
            .path("/tmp/aa")
            .nodeInfo(nodeInfo)
            .buildLeader();
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(Configuration.EMPTY);
      Assertions.assertEquals(
          10,
          dispatcher.partition(
              "topic", new byte[0], new byte[0], ClusterInfoTest.of(List.of(replicaInfo))));
    }
  }

  @Test
  void testParseCostFunctionWeight() {
    var config =
        Configuration.of(
            Map.of(
                "org.astraea.common.cost.BrokerInputCost",
                "20",
                "org.astraea.common.cost.BrokerOutputCost",
                "1.25"));
    var ans = StrictCostDispatcher.parseCostFunctionWeight(config);
    Assertions.assertEquals(2, ans.size());
    for (var entry : ans.entrySet()) {
      if (entry.getKey().getClass().getName().equals("org.astraea.common.cost.BrokerInputCost")) {
        Assertions.assertEquals(20.0, entry.getValue());
      } else if (entry
          .getKey()
          .getClass()
          .getName()
          .equals("org.astraea.common.cost.BrokerOutputCost")) {
        Assertions.assertEquals(1.25, entry.getValue());
      } else {
        Assertions.assertEquals(0.0, entry.getValue());
      }
    }

    // test negative weight
    var config2 =
        Configuration.of(
            Map.of(
                "org.astraea.common.cost.BrokerInputCost",
                "-20",
                "org.astraea.common.cost.BrokerOutputCost",
                "1.25"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> StrictCostDispatcher.parseCostFunctionWeight(config2));
  }

  public static class DumbHasBrokerCost implements HasBrokerCost {

    @Override
    public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
      return Map::of;
    }
  }

  @Test
  void testCostFunctionWithoutFetcher() {
    HasBrokerCost costFunction = (clusterInfo, bean) -> Mockito.mock(BrokerCost.class);
    var replicaInfo0 =
        Replica.builder()
            .topic("topic")
            .partition(0)
            .path("/tmp/aa")
            .nodeInfo(NodeInfo.of(10, "host", 11111))
            .buildLeader();
    var replicaInfo1 =
        Replica.builder()
            .topic("topic")
            .partition(0)
            .path("/tmp/aa")
            .nodeInfo(NodeInfo.of(12, "host2", 11111))
            .buildLeader();
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(Configuration.of((Map.of(DumbHasBrokerCost.class.getName(), "1"))));
      dispatcher.partition(
          "topic",
          new byte[0],
          new byte[0],
          ClusterInfoTest.of(List.of(replicaInfo0, replicaInfo1)));
      Assertions.assertEquals(0, dispatcher.metricCollector.listFetchers().size());
    }
  }

  @Test
  void testEmptyJmxPort() {
    try (var dispatcher = new StrictCostDispatcher()) {

      // pass due to local mbean
      dispatcher.configure(Configuration.of(Map.of(NodeThroughputCost.class.getName(), "1")));
    }
  }

  public static class MyFunction implements HasBrokerCost {
    @Override
    public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
      return () -> Map.of(22, 10D);
    }
  }

  @Test
  void testReturnedPartition() {
    var brokerId = 22;
    var partitionId = 123;
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(Configuration.of(Map.of(MyFunction.class.getName(), "1")));

      var replicaInfo0 =
          Replica.builder()
              .topic("topic")
              .partition(partitionId)
              .path("/tmp/aa")
              .nodeInfo(NodeInfo.of(brokerId, "host", 11111))
              .buildLeader();
      var replicaInfo1 =
          Replica.builder()
              .topic("topic")
              .partition(1)
              .path("/tmp/aa")
              .nodeInfo(NodeInfo.of(1111, "host2", 11111))
              .buildLeader();
      Assertions.assertEquals(
          partitionId,
          dispatcher.partition(
              "topic",
              new byte[0],
              new byte[0],
              ClusterInfoTest.of(List.of(replicaInfo0, replicaInfo1))));
    }
  }

  @Test
  void testDefaultFunction() {
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(Configuration.of(Map.of()));
      Assertions.assertNotEquals(HasBrokerCost.EMPTY, dispatcher.costFunction);
      Assertions.assertEquals(1, dispatcher.metricCollector.listFetchers().size());
    }
  }

  @Test
  void testCostToScore() {
    var cost = Map.of(1, 100D, 2, 10D);
    var score = StrictCostDispatcher.costToScore(() -> cost);
    Assertions.assertTrue(score.get(2) > score.get(1));
  }

  @Test
  void testInvalidCostToScore() {
    Assertions.assertEquals(1, StrictCostDispatcher.costToScore(() -> Map.of(1, 100D)).size());
    Assertions.assertEquals(
        2, StrictCostDispatcher.costToScore(() -> Map.of(1, 100D, 2, 100D)).size());
    var score = StrictCostDispatcher.costToScore(() -> Map.of(1, 100D, 2, 0D));
    Assertions.assertNotEquals(0, score.size());
    Assertions.assertTrue(score.get(2) > score.get(1));
    StrictCostDispatcher.costToScore(() -> Map.of(1, -133D, 2, 100D))
        .values()
        .forEach(v -> Assertions.assertTrue(v > 0));
  }

  @Test
  void testRoundRobinLease() {
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(
          Configuration.of(Map.of(StrictCostDispatcher.ROUND_ROBIN_LEASE_KEY, "2s")));
      Assertions.assertEquals(Duration.ofSeconds(2), dispatcher.roundRobinKeeper.roundRobinLease);

      dispatcher.roundRobinKeeper.tryToUpdate(ClusterInfo.empty(), Lazy.of(Map::of));
      var t = dispatcher.roundRobinKeeper.timeToUpdateRoundRobin;
      var rr =
          Arrays.stream(dispatcher.roundRobinKeeper.roundRobin)
              .boxed()
              .collect(Collectors.toUnmodifiableList());
      Assertions.assertEquals(StrictCostDispatcher.ROUND_ROBIN_LENGTH, rr.size());
      // the rr is not updated yet
      dispatcher.roundRobinKeeper.tryToUpdate(ClusterInfo.empty(), Lazy.of(Map::of));
      IntStream.range(0, rr.size())
          .forEach(
              i -> Assertions.assertEquals(rr.get(i), dispatcher.roundRobinKeeper.roundRobin[i]));
      Utils.sleep(Duration.ofSeconds(3));
      dispatcher.roundRobinKeeper.tryToUpdate(ClusterInfo.empty(), Lazy.of(Map::of));
      // rr is updated already
      Assertions.assertNotEquals(t, dispatcher.roundRobinKeeper.timeToUpdateRoundRobin);
    }
  }

  @Test
  void testTryToUpdateFetcher() {
    try (MBeanClient local = MBeanClient.local()) {
      try (var ignore =
          Mockito.mockStatic(
              MBeanClient.class,
              (invoke) ->
                  invoke.getMethod().getName().equals("jndi") ? local : invoke.callRealMethod())) {
        try (var dispatcher = new StrictCostDispatcher()) {
          var nodeInfo = NodeInfo.of(10, "host", 2222);

          var clusterInfo =
              ClusterInfoTest.of(
                  List.of(
                      Replica.builder()
                          .topic("topic")
                          .partition(0)
                          .nodeInfo(nodeInfo)
                          .path("/tmp/aa")
                          .buildLeader()));

          Assertions.assertEquals(0, dispatcher.metricCollector.listIdentities().size());
          dispatcher.costFunction =
              new HasBrokerCost() {
                @Override
                public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
                  return Map::of;
                }

                @Override
                public Optional<Fetcher> fetcher() {
                  return Optional.of(Mockito.mock(Fetcher.class));
                }
              };
          dispatcher.jmxPortGetter = id -> Optional.of(1111);
          dispatcher.tryToUpdateFetcher(clusterInfo);
          Assertions.assertEquals(1, dispatcher.metricCollector.listIdentities().size());

          dispatcher.tryToUpdateFetcher(clusterInfo);
          Assertions.assertEquals(1, dispatcher.metricCollector.listIdentities().size());
        }
      }
    }
  }
}
