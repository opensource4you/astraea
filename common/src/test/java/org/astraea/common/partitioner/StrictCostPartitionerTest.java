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
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoTest;
import org.astraea.common.admin.Replica;
import org.astraea.common.cost.BrokerCost;
import org.astraea.common.cost.BrokerInputCost;
import org.astraea.common.cost.HasBrokerCost;
import org.astraea.common.cost.NoSufficientMetricsException;
import org.astraea.common.cost.NodeThroughputCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.collector.MetricStore;
import org.astraea.common.producer.ProducerConfigs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class StrictCostPartitionerTest {

  @Test
  void testJmxPort() {
    try (var partitioner = new StrictCostPartitioner()) {
      partitioner.configure(new Configuration(Map.of()));
      Assertions.assertThrows(
          NoSuchElementException.class, () -> partitioner.jmxPortGetter.apply(0));
      partitioner.configure(new Configuration(Map.of(StrictCostPartitioner.JMX_PORT, "12345")));
      Assertions.assertEquals(12345, partitioner.jmxPortGetter.apply(0));
    }
  }

  @Test
  void testNegativeWeight() {
    try (var partitioner = new StrictCostPartitioner()) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              partitioner.configure(
                  new Configuration(
                      Map.of(
                          Partitioner.COST_PREFIX + "." + ReplicaLeaderCost.class.getName(),
                          "-1"))));

      // Test for cost functions configuring
      partitioner.configure(
          new Configuration(
              Map.of(
                  Partitioner.COST_PREFIX + "." + ReplicaLeaderCost.class.getName(),
                  "0.1",
                  Partitioner.COST_PREFIX + "." + BrokerInputCost.class.getName(),
                  "2",
                  "jmx.port",
                  "1111")));
      Assertions.assertNotEquals(HasBrokerCost.EMPTY, partitioner.costFunction);
    }
  }

  @Test
  void testConfigureCostFunctions() {
    try (var partitioner = new StrictCostPartitioner()) {
      partitioner.configure(
          new Configuration(
              Map.of(
                  Partitioner.COST_PREFIX + "." + ReplicaLeaderCost.class.getName(),
                  "0.1",
                  Partitioner.COST_PREFIX + "." + BrokerInputCost.class.getName(),
                  "2",
                  "jmx.port",
                  "1111")));
      Assertions.assertNotEquals(HasBrokerCost.EMPTY, partitioner.costFunction);
    }
  }

  @Test
  void testNoAvailableBrokers() {
    try (var partitioner = new StrictCostPartitioner()) {
      partitioner.configure(Configuration.EMPTY);
      Assertions.assertEquals(
          0, partitioner.partition("topic", new byte[0], new byte[0], ClusterInfo.empty()));
    }
  }

  @Test
  void testSingleBroker() {
    var nodeInfo = Broker.of(10, "host", 11111);
    var replicaInfo =
        Replica.builder()
            .topic("topic")
            .partition(10)
            .path("/tmp/aa")
            .broker(nodeInfo)
            .buildLeader();
    try (var partitioner = new StrictCostPartitioner()) {
      partitioner.configure(Configuration.EMPTY);
      Assertions.assertEquals(
          10,
          partitioner.partition(
              "topic", new byte[0], new byte[0], ClusterInfoTest.of(List.of(replicaInfo))));
    }
  }

  public static class DumbHasBrokerCost implements HasBrokerCost {

    @Override
    public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
      return Map::of;
    }
  }

  @Test
  void testCostFunctionWithoutSensor() {
    var replicaInfo0 =
        Replica.builder()
            .topic("topic")
            .partition(0)
            .path("/tmp/aa")
            .broker(Broker.of(10, "host", 11111))
            .buildLeader();
    var replicaInfo1 =
        Replica.builder()
            .topic("topic")
            .partition(0)
            .path("/tmp/aa")
            .broker(Broker.of(12, "host2", 11111))
            .buildLeader();
    try (var partitioner = new StrictCostPartitioner()) {
      partitioner.configure(
          new Configuration(
              (Map.of(Partitioner.COST_PREFIX + "." + DumbHasBrokerCost.class.getName(), "1"))));
      partitioner.partition(
          "topic",
          new byte[0],
          new byte[0],
          ClusterInfoTest.of(List.of(replicaInfo0, replicaInfo1)));
      Assertions.assertEquals(0, partitioner.metricStore.sensors().size());
    }
  }

  @Test
  void testEmptyJmxPort() {
    try (var partitioner = new StrictCostPartitioner()) {

      // pass due to local mbean
      partitioner.configure(
          new Configuration(
              Map.of(Partitioner.COST_PREFIX + "." + NodeThroughputCost.class.getName(), "1")));
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
    try (var partitioner = new StrictCostPartitioner()) {
      partitioner.configure(
          new Configuration(
              Map.of(Partitioner.COST_PREFIX + "." + MyFunction.class.getName(), "1")));

      var replicaInfo0 =
          Replica.builder()
              .topic("topic")
              .partition(partitionId)
              .path("/tmp/aa")
              .broker(Broker.of(brokerId, "host", 11111))
              .buildLeader();
      var replicaInfo1 =
          Replica.builder()
              .topic("topic")
              .partition(1)
              .path("/tmp/aa")
              .broker(Broker.of(1111, "host2", 11111))
              .buildLeader();
      Assertions.assertEquals(
          partitionId,
          partitioner.partition(
              "topic",
              new byte[0],
              new byte[0],
              ClusterInfoTest.of(List.of(replicaInfo0, replicaInfo1))));
    }
  }

  @Test
  void testDefaultFunction() {
    try (var partitioner = new StrictCostPartitioner()) {
      partitioner.configure(new Configuration(Map.of()));
      Assertions.assertNotEquals(HasBrokerCost.EMPTY, partitioner.costFunction);
      Utils.waitFor(() -> partitioner.metricStore.sensors().size() == 1);
    }
  }

  @Test
  void testCostToScore() {
    var cost = Map.of(1, 100D, 2, 10D);
    var score = StrictCostPartitioner.costToScore(() -> cost);
    Assertions.assertTrue(score.get(2) > score.get(1));
  }

  @Test
  void testInvalidCostToScore() {
    Assertions.assertEquals(1, StrictCostPartitioner.costToScore(() -> Map.of(1, 100D)).size());
    Assertions.assertEquals(
        2, StrictCostPartitioner.costToScore(() -> Map.of(1, 100D, 2, 100D)).size());
    var score = StrictCostPartitioner.costToScore(() -> Map.of(1, 100D, 2, 0D));
    Assertions.assertNotEquals(0, score.size());
    Assertions.assertTrue(score.get(2) > score.get(1));
    StrictCostPartitioner.costToScore(() -> Map.of(1, -133D, 2, 100D))
        .values()
        .forEach(v -> Assertions.assertTrue(v > 0));
  }

  @Test
  void testRoundRobinLease() {
    try (var partitioner = new StrictCostPartitioner()) {
      partitioner.configure(
          new Configuration(Map.of(StrictCostPartitioner.ROUND_ROBIN_LEASE_KEY, "2s")));
      Assertions.assertEquals(Duration.ofSeconds(2), partitioner.roundRobinKeeper.roundRobinLease);

      partitioner.roundRobinKeeper.tryToUpdate(ClusterInfo.empty(), Map::of);
      var t = partitioner.roundRobinKeeper.lastUpdated.get();
      var rr =
          Arrays.stream(partitioner.roundRobinKeeper.roundRobin)
              .boxed()
              .collect(Collectors.toUnmodifiableList());
      Assertions.assertEquals(StrictCostPartitioner.ROUND_ROBIN_LENGTH, rr.size());
      // the rr is not updated yet
      partitioner.roundRobinKeeper.tryToUpdate(ClusterInfo.empty(), Map::of);
      IntStream.range(0, rr.size())
          .forEach(
              i -> Assertions.assertEquals(rr.get(i), partitioner.roundRobinKeeper.roundRobin[i]));
      Utils.sleep(Duration.ofSeconds(3));
      partitioner.roundRobinKeeper.tryToUpdate(ClusterInfo.empty(), Map::of);
      // rr is updated already
      Assertions.assertNotEquals(t, partitioner.roundRobinKeeper.lastUpdated.get());
    }
  }

  @Test
  void testCostNoSufficientMetricException() {
    try (var partitioner = new StrictCostPartitioner()) {
      partitioner.configure(Configuration.EMPTY);
      // The cost function always throws exception
      partitioner.costFunction =
          new HasBrokerCost() {
            @Override
            public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
              throw new NoSufficientMetricsException(this, Duration.ZERO);
            }
          };
      var clusterInfo = Mockito.mock(ClusterInfo.class);
      var replicaLeader1 = Mockito.mock(Replica.class);
      var replicaLeader2 = Mockito.mock(Replica.class);
      Mockito.when(clusterInfo.replicaLeaders("topicA"))
          .thenReturn(List.of(replicaLeader1, replicaLeader2));

      try {
        partitioner.partition("topic", new byte[0], new byte[0], clusterInfo);
      } catch (NoSufficientMetricsException e) {
        Assertions.fail();
      }
    }
  }

  /** Test if the partitioner use correct metric store */
  @Test
  void testMetricStoreConfigure() {
    try (var mockedReceiver = Mockito.mockStatic(MetricStore.Receiver.class)) {
      var topicReceiverCount = new AtomicInteger(0);
      var localReceiverCount = new AtomicInteger(0);
      mockedReceiver
          .when(() -> MetricStore.Receiver.topic(Mockito.any()))
          .then(
              (Answer<MetricStore.Receiver>)
                  invocation -> {
                    topicReceiverCount.incrementAndGet();
                    return Mockito.mock(MetricStore.Receiver.class);
                  });
      mockedReceiver
          .when(() -> MetricStore.Receiver.local(Mockito.any()))
          .then(
              (Answer<MetricStore.Receiver>)
                  invocation -> {
                    localReceiverCount.incrementAndGet();
                    return Mockito.mock(MetricStore.Receiver.class);
                  });

      try (var partitioner = new StrictCostPartitioner()) {
        // Check default metric store
        var config =
            new Configuration(Map.of(ProducerConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
        partitioner.configure(config);
        Assertions.assertNotEquals(0, localReceiverCount.get());
        Assertions.assertEquals(0, topicReceiverCount.get());

        // Check topic metric store
        localReceiverCount.set(0);
        topicReceiverCount.set(0);
        config =
            new Configuration(
                Map.of(
                    ProducerConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092",
                    StrictCostPartitioner.METRIC_STORE_KEY,
                    StrictCostPartitioner.METRIC_STORE_TOPIC));
        partitioner.configure(config);
        Assertions.assertNotEquals(0, topicReceiverCount.get());
        // topic collector may use local receiver to get local jmx metric

        // Check local metric store
        topicReceiverCount.set(0);
        localReceiverCount.set(0);
        config =
            new Configuration(
                Map.of(
                    ProducerConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092",
                    StrictCostPartitioner.METRIC_STORE_KEY,
                    StrictCostPartitioner.METRIC_STORE_LOCAL));
        partitioner.configure(config);
        Assertions.assertNotEquals(0, localReceiverCount.get());
        Assertions.assertEquals(0, topicReceiverCount.get());

        // Check unknown metric store
        localReceiverCount.set(0);
        topicReceiverCount.set(0);
        var config2 =
            new Configuration(
                Map.of(
                    ProducerConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092",
                    StrictCostPartitioner.METRIC_STORE_KEY,
                    "unknown"));
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> partitioner.configure(config2));
      }
    }
  }
}
