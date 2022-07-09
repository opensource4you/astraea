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
package org.astraea.app.partitioner;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.cost.BrokerCost;
import org.astraea.app.cost.BrokerInputCost;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.cost.ReplicaLeaderCost;
import org.astraea.app.cost.ThroughputCost;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.collector.Receiver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StrictCostDispatcherTest {

  @Test
  void testClose() {
    var dispatcher = new StrictCostDispatcher();
    var receiver = Mockito.mock(Receiver.class);
    dispatcher.receivers.put(10, receiver);
    dispatcher.close();
    Mockito.verify(receiver, Mockito.times(1)).close();
    Assertions.assertEquals(0, dispatcher.receivers.size());
  }

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
                  Configuration.of(Map.of(ReplicaLeaderCost.NoMetrics.class.getName(), "-1"))));

      // Test for cost functions configuring
      dispatcher.configure(
          Configuration.of(
              Map.of(
                  ReplicaLeaderCost.NoMetrics.class.getName(),
                  "0.1",
                  BrokerInputCost.class.getName(),
                  "2",
                  "jmx.port",
                  "1111")));
      Assertions.assertEquals(2, dispatcher.functions.size());
    }
  }

  @Test
  void testConfigureCostFunctions() {
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(
          Configuration.of(
              Map.of(
                  ReplicaLeaderCost.NoMetrics.class.getName(),
                  "0.1",
                  BrokerInputCost.class.getName(),
                  "2",
                  "jmx.port",
                  "1111")));
      Assertions.assertEquals(2, dispatcher.functions.size());
    }
  }

  @Test
  void testNoAvailableBrokers() {
    var clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.availableReplicaLeaders(Mockito.anyString())).thenReturn(List.of());
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(Map.of(), Optional.empty(), Map.of());
      Assertions.assertEquals(
          0, dispatcher.partition("topic", new byte[0], new byte[0], clusterInfo));
    }
  }

  @Test
  void testSingleBroker() {
    var nodeInfo = NodeInfo.of(10, "host", 11111);
    var replicaInfo = ReplicaInfo.of("topic", 10, nodeInfo, true, true, true);
    var clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.availableReplicaLeaders(Mockito.anyString()))
        .thenReturn(List.of(replicaInfo));
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(Map.of(), Optional.empty(), Map.of());
      Assertions.assertEquals(
          10, dispatcher.partition("topic", new byte[0], new byte[0], clusterInfo));
    }
  }

  @Test
  void testParseCostFunctionWeight() {
    var config =
        Configuration.of(
            Map.of(
                "org.astraea.app.cost.BrokerInputCost",
                "20",
                "org.astraea.app.cost.BrokerOutputCost",
                "1.25"));
    var ans = StrictCostDispatcher.parseCostFunctionWeight(config);
    Assertions.assertEquals(2, ans.size());
    for (var entry : ans.entrySet()) {
      if (entry.getKey().getClass().getName().equals("org.astraea.app.cost.BrokerInputCost")) {
        Assertions.assertEquals(20.0, entry.getValue());
      } else if (entry
          .getKey()
          .getClass()
          .getName()
          .equals("org.astraea.app.cost.BrokerOutputCost")) {
        Assertions.assertEquals(1.25, entry.getValue());
      } else {
        Assertions.assertEquals(0.0, entry.getValue());
      }
    }

    // test negative weight
    var config2 =
        Configuration.of(
            Map.of(
                "org.astraea.app.cost.BrokerInputCost",
                "-20",
                "org.astraea.app.cost.BrokerOutputCost",
                "1.25"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> StrictCostDispatcher.parseCostFunctionWeight(config2));
  }

  @Test
  void testCostFunctionWithoutFetcher() {
    var costFunction = new CostFunction() {};
    var replicaInfo0 = ReplicaInfo.of("topic", 0, NodeInfo.of(10, "host", 11111), true, true, true);
    var replicaInfo1 =
        ReplicaInfo.of("topic", 1, NodeInfo.of(12, "host2", 11111), true, true, true);
    var clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.availableReplicaLeaders(Mockito.anyString()))
        .thenReturn(List.of(replicaInfo0, replicaInfo1));
    Mockito.when(clusterInfo.clusterBean()).thenReturn(ClusterBean.of(Map.of()));
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(Map.of(costFunction, 1D), Optional.empty(), Map.of());
      dispatcher.partition("topic", new byte[0], new byte[0], clusterInfo);
      Assertions.assertNotNull(dispatcher.roundRobin);
      Assertions.assertEquals(0, dispatcher.receivers.size());
    }
  }

  @Test
  void testReceivers() {
    var costFunction =
        new CostFunction() {
          @Override
          public Optional<Fetcher> fetcher() {
            return Optional.of(Mockito.mock(Fetcher.class));
          }
        };
    var jmxPort = 12345;
    var count = new AtomicInteger();
    var dispatcher =
        new StrictCostDispatcher() {
          @Override
          Receiver receiver(String host, int port, Fetcher fetcher) {
            Assertions.assertEquals(jmxPort, port);
            count.incrementAndGet();
            return Mockito.mock(Receiver.class);
          }
        };
    dispatcher.configure(Map.of(costFunction, 1D), Optional.of(jmxPort), Map.of());
    var replicaInfo0 = ReplicaInfo.of("topic", 0, NodeInfo.of(10, "host", 11111), true, true, true);
    var replicaInfo1 = ReplicaInfo.of("topic", 1, NodeInfo.of(10, "host", 11111), true, true, true);
    var replicaInfo2 =
        ReplicaInfo.of("topic", 1, NodeInfo.of(11, "host2", 11111), true, true, true);
    var clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.availableReplicaLeaders(Mockito.anyString()))
        .thenReturn(List.of(replicaInfo0, replicaInfo1, replicaInfo2));
    Mockito.when(clusterInfo.clusterBean()).thenReturn(ClusterBean.of(Map.of()));
    // there is one local receiver by default
    Assertions.assertEquals(1, dispatcher.receivers.size());
    Assertions.assertEquals(-1, dispatcher.receivers.keySet().iterator().next());

    // generate two receivers since there are two brokers (hosting three replicas)
    dispatcher.partition("topic", new byte[0], new byte[0], clusterInfo);
    Assertions.assertEquals(3, dispatcher.receivers.size());
    Assertions.assertEquals(2, count.get());

    // all brokers have receivers already so no new receiver is born
    dispatcher.partition("topic", new byte[0], new byte[0], clusterInfo);
    Assertions.assertEquals(3, dispatcher.receivers.size());
    Assertions.assertEquals(2, count.get());
  }

  @Test
  void testEmptyJmxPort() {
    var dispatcher = new StrictCostDispatcher();

    // pass due to local mbean
    dispatcher.configure(Map.of(new ThroughputCost(), 1D), Optional.empty(), Map.of());

    // pass due to default port
    dispatcher.configure(Map.of(new ThroughputCost(), 1D), Optional.of(111), Map.of());

    // pass due to specify port
    dispatcher.configure(Map.of(new ThroughputCost(), 1D), Optional.empty(), Map.of(222, 111));
  }

  @Test
  void testReturnedPartition() {
    var brokerId = 22;
    var partitionId = 123;
    var dispatcher = new StrictCostDispatcher();
    var costFunction =
        new HasBrokerCost() {
          @Override
          public BrokerCost brokerCost(ClusterInfo clusterInfo) {
            return () -> Map.of(brokerId, 10D);
          }
        };
    dispatcher.configure(Map.of(costFunction, 1D), Optional.empty(), Map.of());

    var replicaInfo0 =
        ReplicaInfo.of(
            "topic", partitionId, NodeInfo.of(brokerId, "host", 11111), true, true, true);
    var replicaInfo1 =
        ReplicaInfo.of("topic", 1, NodeInfo.of(1111, "host2", 11111), true, true, true);
    var clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.availableReplicaLeaders(Mockito.anyString()))
        .thenReturn(List.of(replicaInfo0, replicaInfo1));
    Mockito.when(clusterInfo.clusterBean()).thenReturn(ClusterBean.of(Map.of()));

    Assertions.assertEquals(
        partitionId, dispatcher.partition("topic", new byte[0], new byte[0], clusterInfo));
  }

  @Test
  void testDefaultFunction() {
    var dispatcher = new StrictCostDispatcher();
    dispatcher.configure(Configuration.of(Map.of()));
    Assertions.assertEquals(1, dispatcher.functions.size());
    Assertions.assertEquals(1, dispatcher.receivers.size());
  }

  @Test
  void testCostToScore() {
    var cost = Map.of(1, 100D, 2, 10D);
    var score = StrictCostDispatcher.costToScore(cost);
    Assertions.assertTrue(score.get(2) > score.get(1));
  }
}
