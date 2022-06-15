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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.cost.BrokerCost;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.cost.NodeInfo;
import org.astraea.app.cost.ReplicaInfo;
import org.astraea.app.cost.ThroughputCost;
import org.astraea.app.cost.broker.BrokerInputCost;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.collector.Receiver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StrictCostDispatcherTest {

  private static ReplicaInfo createFakeReplicaLeaderInfo(
      String topic, int partition, NodeInfo replicaNode) {
    return ReplicaInfo.of(topic, partition, replicaNode, true, true, false);
  }

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
  void testConfigure() {
    try (var dispatcher = new StrictCostDispatcher()) {
      dispatcher.configure(Configuration.of(Map.of()));
      Assertions.assertThrows(NoSuchElementException.class, () -> dispatcher.jmxPort(0));
      dispatcher.configure(Configuration.of(Map.of(StrictCostDispatcher.JMX_PORT, "12345")));
      Assertions.assertEquals(12345, dispatcher.jmxPort(0));
    }

    try (var dispatcher = new StrictCostDispatcher()) {
      // Test for negative weight.
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              dispatcher.configure(Configuration.of(Map.of(ThroughputCost.class.getName(), "-1"))));

      // Test for cost functions configuring
      dispatcher.configure(
          Configuration.of(
              Map.of(ThroughputCost.class.getName(), "0.1", BrokerInputCost.class.getName(), "2")));
      Assertions.assertEquals(2, dispatcher.functions.size());
    }
  }

  @Test
  void testBestPartition() {
    var partitions =
        List.of(
            createFakeReplicaLeaderInfo("t", 0, NodeInfo.of(10, "h0", 1000)),
            createFakeReplicaLeaderInfo("t", 1, NodeInfo.of(11, "h1", 1000)));

    var score = List.of(Map.of(10, 0.8D), Map.of(11, 0.7D));

    var result = StrictCostDispatcher.bestPartition(partitions, score).get();

    Assertions.assertEquals(0.7D, result.getValue());
    Assertions.assertEquals(1, result.getKey().partition());
  }

  @Test
  void testPartition() {
    // n0 is busy
    var n0 = NodeInfo.of(10, "host0", 12345);
    var p0 = createFakeReplicaLeaderInfo("aa", 1, n0);
    // n1 is free
    var n1 = NodeInfo.of(11, "host1", 12345);
    var p1 = createFakeReplicaLeaderInfo("aa", 2, n1);

    var receiver = Mockito.mock(Receiver.class);
    Mockito.when(receiver.current()).thenReturn(List.of());

    var costFunction1 =
        new HasBrokerCost() {
          @Override
          public BrokerCost brokerCost(ClusterInfo clusterInfo) {
            var brokerCost =
                clusterInfo.beans().all().keySet().stream()
                    .collect(
                        Collectors.toMap(
                            Function.identity(), id -> id.equals(n0.id()) ? 0.9D : 0.5D));
            return () -> brokerCost;
          }

          @Override
          public Fetcher fetcher() {
            return client -> List.of();
          }
        };
    var costFunction2 =
        new HasBrokerCost() {
          @Override
          public BrokerCost brokerCost(ClusterInfo clusterInfo) {
            var brokerCost =
                clusterInfo.beans().all().keySet().stream()
                    .collect(
                        Collectors.toMap(
                            Function.identity(), id -> id.equals(n0.id()) ? 0.6D : 0.8D));
            return () -> brokerCost;
          }

          @Override
          public Fetcher fetcher() {
            return client -> List.of();
          }
        };

    try (var dispatcher =
        new StrictCostDispatcher(List.of(costFunction1, costFunction2)) {
          @Override
          Receiver receiver(String host, int port) {
            return receiver;
          }

          @Override
          int jmxPort(int id) {
            return 0;
          }
        }) {
      var clusterInfo = Mockito.mock(ClusterInfo.class);

      // there is no available partition
      Mockito.when(clusterInfo.availableReplicaLeaders("aa")).thenReturn(List.of());
      Mockito.when(clusterInfo.topics()).thenReturn(Set.of("aa"));
      Mockito.when(clusterInfo.beans()).thenReturn(ClusterBean.of(Map.of()));
      Assertions.assertEquals(0, dispatcher.partition("aa", new byte[0], new byte[0], clusterInfo));

      // there is only one available partition
      Mockito.when(clusterInfo.availableReplicaLeaders("aa")).thenReturn(List.of(p0));
      Assertions.assertEquals(1, dispatcher.partition("aa", new byte[0], new byte[0], clusterInfo));

      // there is no beans, so it just returns the partition.
      Mockito.when(clusterInfo.availableReplicaLeaders("aa")).thenReturn(List.of(p0, p1));
      Assertions.assertEquals(2, dispatcher.partition("aa", new byte[0], new byte[0], clusterInfo));

      // cost functions with weight
      dispatcher.functions = Map.of(costFunction1, 0.1, costFunction2, 0.9);
      Assertions.assertEquals(1, dispatcher.partition("aa", new byte[0], new byte[0], clusterInfo));
    }
  }

  @Test
  void testParseCostFunctionWeight() {
    var config =
        Configuration.of(
            Map.of(
                "org.astraea.app.cost.broker.BrokerInputCost",
                "20",
                "org.astraea.app.cost.broker.BrokerOutputCost",
                "1.25"));
    var ans = StrictCostDispatcher.parseCostFunctionWeight(config);
    Assertions.assertEquals(2, ans.size());
    for (var entry : ans.entrySet()) {
      if (entry
          .getKey()
          .getClass()
          .getName()
          .equals("org.astraea.app.cost.broker.BrokerInputCost")) {
        Assertions.assertEquals(20.0, entry.getValue());
      } else if (entry
          .getKey()
          .getClass()
          .getName()
          .equals("org.astraea.app.cost.broker.BrokerOutputCost")) {
        Assertions.assertEquals(1.25, entry.getValue());
      } else {
        Assertions.assertEquals(0.0, entry.getValue());
      }
    }

    // test negative weight
    var config2 =
        Configuration.of(
            Map.of(
                "org.astraea.app.cost.broker.BrokerInputCost",
                "-20",
                "org.astraea.app.cost.broker.BrokerOutputCost",
                "1.25"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> StrictCostDispatcher.parseCostFunctionWeight(config2));
  }
}
