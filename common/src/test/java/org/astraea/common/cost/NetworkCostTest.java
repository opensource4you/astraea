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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.DataRate;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoBuilder;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.tweakers.ShuffleTweaker;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class NetworkCostTest {

  private static NetworkIngressCost ingressCost() {
    return new NetworkIngressCost() {
      @Override
      void updateCurrentCluster(
          ClusterInfo<Replica> clusterInfo,
          ClusterBean clusterBean,
          AtomicReference<ClusterInfo<Replica>> ref) {
        ref.compareAndSet(null, clusterInfo);
      }
    };
  }

  private static NetworkEgressCost egressCost() {
    return new NetworkEgressCost() {
      @Override
      void updateCurrentCluster(
          ClusterInfo<Replica> clusterInfo,
          ClusterBean clusterBean,
          AtomicReference<ClusterInfo<Replica>> ref) {
        ref.compareAndSet(null, clusterInfo);
      }
    };
  }

  @Test
  void testInitialClusterInfo() {
    var cost = new NetworkIngressCost();
    var initial =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1))
            .addFolders(Map.of(1, Set.of("/folder0", "/folder1")))
            .addTopic("Pipeline", 1, (short) 1)
            .addNode(Set.of(2))
            .addFolders(Map.of(2, Set.of("/folder0", "/folder1")))
            .build();
    var modified = new ShuffleTweaker(10, 30).generate(initial).findFirst().orElseThrow();
    var withLog =
        ClusterBean.of(
            Map.of(
                1,
                List.of(
                    logSize(TopicPartition.of("Pipeline", 0), -1),
                    bandwidth(ServerMetrics.Topic.BYTES_IN_PER_SEC, "Pipeline", 0),
                    bandwidth(ServerMetrics.Topic.BYTES_OUT_PER_SEC, "Pipeline", 0))));
    var noLog =
        ClusterBean.of(
            Map.of(
                1,
                List.of(
                    bandwidth(ServerMetrics.Topic.BYTES_IN_PER_SEC, "Pipeline", 0),
                    bandwidth(ServerMetrics.Topic.BYTES_OUT_PER_SEC, "Pipeline", 0))));

    Assertions.assertThrows(
        NoSufficientMetricsException.class, () -> cost.clusterCost(initial, ClusterBean.EMPTY));
    Assertions.assertThrows(
        NoSufficientMetricsException.class, () -> cost.clusterCost(modified, ClusterBean.EMPTY));
    Assertions.assertThrows(
        NoSufficientMetricsException.class, () -> cost.clusterCost(initial, noLog));
    Assertions.assertThrows(
        NoSufficientMetricsException.class, () -> cost.clusterCost(modified, noLog));
    Assertions.assertDoesNotThrow(() -> cost.clusterCost(initial, withLog));
    Assertions.assertDoesNotThrow(() -> cost.clusterCost(modified, noLog));
  }

  @Test
  void testEstimatedIngressRate() {
    testEstimatedRate(ServerMetrics.Topic.BYTES_IN_PER_SEC);
  }

  @Test
  void testEstimatedEgressRate() {
    testEstimatedRate(ServerMetrics.Topic.BYTES_OUT_PER_SEC);
  }

  void testEstimatedRate(ServerMetrics.Topic metric) {
    var t = new HandWrittenTestCase();
    new NetworkIngressCost()
        .estimateRate(t.clusterInfo(), t.clusterBean(), metric)
        .forEach(
            (tp, actual) -> {
              var expected = t.expectedRate().get(tp);
              var test = (expected - 3) + " < " + actual + " < " + (expected + 3);
              System.out.println(test);
              Assertions.assertTrue((expected - 3) < actual && actual < (expected + 3), test);
            });
  }

  static Stream<Arguments> testcases() {
    return Stream.of(
        Arguments.of(ingressCost(), new HandWrittenTestCase()),
        Arguments.of(ingressCost(), new LargeTestCase(2, 100, 0xfeedbabe)),
        Arguments.of(ingressCost(), new LargeTestCase(3, 100, 0xabcdef01)),
        Arguments.of(ingressCost(), new LargeTestCase(10, 200, 0xcafebabe)),
        Arguments.of(ingressCost(), new LargeTestCase(15, 300, 0xfee1dead)),
        Arguments.of(egressCost(), new HandWrittenTestCase()),
        Arguments.of(egressCost(), new LargeTestCase(5, 100, 0xfa11fa11)),
        Arguments.of(egressCost(), new LargeTestCase(6, 100, 0xf001f001)),
        Arguments.of(egressCost(), new LargeTestCase(8, 200, 0xba1aba1a)),
        Arguments.of(egressCost(), new LargeTestCase(14, 300, 0xdd0000bb)));
  }

  @ParameterizedTest
  @MethodSource("testcases")
  @DisplayName("Run with Balancer")
  void testOptimization(HasClusterCost costFunction, TestCase testcase) {
    var newPlan =
        Balancer.Official.Greedy.create(
                AlgorithmConfig.builder()
                    .clusterCost(costFunction)
                    .metricSource(testcase::clusterBean)
                    .build())
            .offer(testcase.clusterInfo(), Duration.ofSeconds(1));

    Assertions.assertTrue(newPlan.solution().isPresent());
    System.out.println("Initial cost: " + newPlan.initialClusterCost().value());
    System.out.println("New cost: " + newPlan.solution().get().proposalClusterCost().value());
    Assertions.assertTrue(
        newPlan.initialClusterCost().value()
            > newPlan.solution().get().proposalClusterCost().value());
  }

  @Test
  void testCompositeOptimization() {
    var testCase = new LargeTestCase(10, 300, 0xfeed);
    var costFunction =
        HasClusterCost.of(
            Map.of(
                ingressCost(), 1.0,
                egressCost(), 1.0));
    testOptimization(costFunction, testCase);
  }

  @Test
  void testReplicationAware() {
    var base =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(Map.of(1, Set.of("/folder0", "/folder1")))
            .addFolders(Map.of(2, Set.of("/folder0", "/folder1")))
            .addFolders(Map.of(3, Set.of("/folder0", "/folder1")))
            .build();
    var iter1 = List.of(1, 2).iterator();
    var iter2 = List.of(true, false).iterator();
    var iter3 = List.of(true, false).iterator();
    var iter4 = List.of(3, 1).iterator();
    var iter5 = List.of(true, false).iterator();
    var iter6 = List.of(true, false).iterator();
    var cluster =
        ClusterInfoBuilder.builder(base)
            .addTopic(
                "Rain",
                1,
                (short) 2,
                (r) ->
                    Replica.builder(r)
                        .nodeInfo(base.node(iter1.next()))
                        .isPreferredLeader(iter2.next())
                        .isLeader(iter3.next())
                        .size(1)
                        .build())
            .addTopic(
                "Drop",
                1,
                (short) 2,
                (r) ->
                    Replica.builder(r)
                        .nodeInfo(base.node(iter4.next()))
                        .isPreferredLeader(iter5.next())
                        .isLeader(iter6.next())
                        .size(1)
                        .build())
            .build();
    var beans =
        ClusterBean.of(
            Map.of(
                1,
                    List.of(
                        bandwidth(ServerMetrics.Topic.BYTES_IN_PER_SEC, "Rain", 100),
                        bandwidth(ServerMetrics.Topic.BYTES_OUT_PER_SEC, "Rain", 300)),
                2, List.of(),
                3,
                    List.of(
                        bandwidth(ServerMetrics.Topic.BYTES_IN_PER_SEC, "Drop", 80),
                        bandwidth(ServerMetrics.Topic.BYTES_OUT_PER_SEC, "Drop", 800))));

    double error = 1e-9;
    Function<Double, Predicate<Double>> around =
        (expected) -> (value) -> (expected + error) >= value && value >= (expected - error);

    double expectedIngress1 = 100 + 80;
    double expectedIngress2 = 100;
    double expectedIngress3 = 80;
    double expectedEgress1 = 300 + 100;
    double expectedEgress2 = 0;
    double expectedEgress3 = 800 + 80 * 2;
    double expectedIngressScore = (expectedIngress1 - expectedIngress3) / expectedIngress1;
    double expectedEgressScore = (expectedEgress3 - expectedEgress2) / expectedEgress3;
    double ingressScore = ingressCost().clusterCost(cluster, beans).value();
    double egressScore = egressCost().clusterCost(cluster, beans).value();
    Assertions.assertTrue(
        around.apply(expectedIngressScore).test(ingressScore),
        "Ingress score should be " + expectedIngressScore + " but it is " + ingressScore);
    Assertions.assertTrue(
        around.apply(expectedEgressScore).test(egressScore),
        "Egress score should be " + expectedEgressScore + " but it is " + egressScore);
  }

  interface TestCase {

    ClusterInfo<Replica> clusterInfo();

    ClusterBean clusterBean();
  }

  /**
   * A manually crafted cluster & metrics
   *
   * <ul>
   *   <li>3 Brokers.
   *   <li>Broker 3 has no load at all.
   * </ul>
   */
  private static class HandWrittenTestCase implements TestCase {
    private HandWrittenTestCase() {
      this.clusterBean =
          ClusterBean.of(
              Map.of(
                  1,
                  expectedBrokerTopicBandwidth.get(1).entrySet().stream()
                      .flatMap(
                          e ->
                              Stream.of(
                                  bandwidth(
                                      ServerMetrics.Topic.BYTES_IN_PER_SEC,
                                      e.getKey(),
                                      e.getValue()),
                                  bandwidth(
                                      ServerMetrics.Topic.BYTES_OUT_PER_SEC,
                                      e.getKey(),
                                      e.getValue())))
                      .collect(Collectors.toUnmodifiableList()),
                  2,
                  expectedBrokerTopicBandwidth.get(2).entrySet().stream()
                      .flatMap(
                          e ->
                              Stream.of(
                                  bandwidth(
                                      ServerMetrics.Topic.BYTES_IN_PER_SEC,
                                      e.getKey(),
                                      e.getValue()),
                                  bandwidth(
                                      ServerMetrics.Topic.BYTES_OUT_PER_SEC,
                                      e.getKey(),
                                      e.getValue())))
                      .collect(Collectors.toUnmodifiableList()),
                  3,
                  expectedBrokerTopicBandwidth.get(3).entrySet().stream()
                      .flatMap(
                          e ->
                              Stream.of(
                                  bandwidth(
                                      ServerMetrics.Topic.BYTES_IN_PER_SEC,
                                      e.getKey(),
                                      e.getValue()),
                                  bandwidth(
                                      ServerMetrics.Topic.BYTES_OUT_PER_SEC,
                                      e.getKey(),
                                      e.getValue())))
                      .collect(Collectors.toUnmodifiableList())));
    }

    final ClusterInfo<Replica> base =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(Map.of(1, Set.of("/ssd1", "/ssd2", "/ssd3")))
            .addFolders(Map.of(2, Set.of("/ssd1", "/ssd2", "/ssd3")))
            .addFolders(Map.of(3, Set.of("/ssd1", "/ssd2", "/ssd3")))
            .build();
    final Map<TopicPartition, Long> expectedRate =
        Map.of(
            TopicPartition.of("Beef", 0), ThreadLocalRandom.current().nextLong(10000),
            TopicPartition.of("Beef", 1), ThreadLocalRandom.current().nextLong(10000),
            TopicPartition.of("Beef", 2), 0L,
            TopicPartition.of("Beef", 3), ThreadLocalRandom.current().nextLong(10000),
            TopicPartition.of("Pork", 0), ThreadLocalRandom.current().nextLong(10000),
            TopicPartition.of("Pork", 1), ThreadLocalRandom.current().nextLong(10000),
            TopicPartition.of("Pork", 2), 0L,
            TopicPartition.of("Pork", 3), ThreadLocalRandom.current().nextLong(10000));

    final Map<Integer, Map<String, Long>> expectedBrokerTopicBandwidth =
        Stream.of(0, 1, 2)
            .collect(
                Collectors.toUnmodifiableMap(
                    index -> index + 1,
                    index ->
                        Stream.of("Beef", "Pork")
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    topic -> topic,
                                    topic ->
                                        expectedRate.entrySet().stream()
                                            .filter(e -> e.getKey().topic().equals(topic))
                                            .filter(e -> e.getKey().partition() % 3 == index)
                                            .mapToLong(Map.Entry::getValue)
                                            .sum()))));
    final Function<Replica, Replica> modPlacement =
        replica ->
            Replica.builder(replica)
                .size(expectedRate.get(replica.topicPartition()) * 100)
                .nodeInfo(base.node(1 + replica.partition() % 3))
                .build();
    final ClusterInfo<Replica> clusterInfo =
        ClusterInfoBuilder.builder(base)
            .addTopic("Beef", 4, (short) 1, modPlacement)
            .addTopic("Pork", 4, (short) 1, modPlacement)
            .build();
    final ClusterBean clusterBean;

    public Map<TopicPartition, Long> expectedRate() {
      return expectedRate;
    }

    @Override
    public ClusterInfo<Replica> clusterInfo() {
      return clusterInfo;
    }

    @Override
    public ClusterBean clusterBean() {
      return clusterBean;
    }
  }

  /** A large cluster */
  private static class LargeTestCase implements TestCase {

    private final ClusterInfo<Replica> clusterInfo;
    private final ClusterBean clusterBean;
    private final Map<TopicPartition, Long> rate;
    private final Supplier<DataRate> dataRateSupplier;

    public LargeTestCase(int brokers, int partitions, int seed) {
      var random = new Random(seed);
      this.dataRateSupplier =
          () -> {
            switch (1 + random.nextInt(8)) {
              case 1:
              case 2:
              case 3:
              case 4:
              case 5:
                return DataRate.MB.of((long) (random.nextInt(50))).perSecond();
              case 6:
              case 7:
                return DataRate.MB.of(3 * (long) (random.nextInt(50))).perSecond();
              case 8:
                return DataRate.MB.of(5 * (long) (random.nextInt(50))).perSecond();
              default:
                throw new RuntimeException();
            }
          };
      this.clusterInfo =
          ClusterInfoBuilder.builder()
              .addNode(IntStream.range(0, brokers).boxed().collect(Collectors.toUnmodifiableSet()))
              .addFolders(
                  IntStream.range(0, brokers)
                      .boxed()
                      .collect(
                          Collectors.toUnmodifiableMap(
                              b -> b,
                              b -> Set.of("/broker" + b + "/ssd1", "/broker" + b + "/ssd2"))))
              .addTopic(
                  "Pipeline",
                  partitions,
                  (short) 1,
                  replica ->
                      Replica.builder(replica)
                          .size((long) (24 * 60 * 60 * dataRateSupplier.get().byteRate()))
                          .build())
              .build();
      this.rate =
          clusterInfo.topicPartitions().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      tp -> tp,
                      tp -> clusterInfo.replicaLeader(tp).orElseThrow().size() / 24 / 60 / 60));
      var consumerFanout =
          IntStream.range(0, partitions)
              .boxed()
              .collect(
                  Collectors.toUnmodifiableMap(
                      p -> TopicPartition.of("Pipeline", p), p -> random.nextInt(10)));
      this.clusterBean =
          ClusterBean.of(
              IntStream.range(0, brokers)
                  .boxed()
                  .collect(
                      Collectors.toUnmodifiableMap(
                          id -> id,
                          id ->
                              List.of(
                                  noise(random.nextInt()),
                                  noise(random.nextInt()),
                                  noise(random.nextInt()),
                                  noise(random.nextInt()),
                                  bandwidth(
                                      ServerMetrics.Topic.BYTES_IN_PER_SEC,
                                      "Pipeline",
                                      clusterInfo
                                          .replicaStream()
                                          .filter(r -> r.nodeInfo().id() == id)
                                          .filter(ReplicaInfo::isLeader)
                                          .filter(ReplicaInfo::isOnline)
                                          .mapToLong(r -> rate.get(r.topicPartition()))
                                          .sum()),
                                  noise(random.nextInt()),
                                  noise(random.nextInt()),
                                  bandwidth(
                                      ServerMetrics.Topic.BYTES_OUT_PER_SEC,
                                      "Pipeline",
                                      clusterInfo
                                          .replicaStream()
                                          .filter(r -> r.nodeInfo().id() == id)
                                          .filter(ReplicaInfo::isLeader)
                                          .filter(ReplicaInfo::isOnline)
                                          .mapToLong(
                                              r ->
                                                  rate.get(r.topicPartition())
                                                      * consumerFanout.get(r.topicPartition()))
                                          .sum()),
                                  noise(random.nextInt())))));
    }

    @Override
    public ClusterInfo<Replica> clusterInfo() {
      return clusterInfo;
    }

    @Override
    public ClusterBean clusterBean() {
      return clusterBean;
    }
  }

  static ServerMetrics.Topic.Meter noise(int seed) {
    var random = new Random(seed);
    return bandwidth(
        random.nextInt() > 0
            ? ServerMetrics.Topic.TOTAL_PRODUCE_REQUESTS_PER_SEC
            : ServerMetrics.Topic.TOTAL_FETCH_REQUESTS_PER_SEC,
        "noise_" + random.nextInt(),
        random.nextDouble());
  }

  static ServerMetrics.Topic.Meter bandwidth(
      ServerMetrics.Topic metric, String topic, double fifteenRate) {
    if (metric == null) return noise(0);
    var domainName = "kafka.server";
    var properties =
        Map.of("type", "BrokerTopicMetric", "topic", topic, "name", metric.metricName());
    var attributes = Map.<String, Object>of("FifteenMinuteRate", fifteenRate);
    return new ServerMetrics.Topic.Meter(new BeanObject(domainName, properties, attributes));
  }

  static LogMetrics.Log.Gauge logSize(TopicPartition topicPartition, long size) {
    var domainName = LogMetrics.DOMAIN_NAME;
    var properties =
        Map.of(
            "type", "BrokerTopicMetric",
            "topic", topicPartition.topic(),
            "partition", String.valueOf(topicPartition.partition()),
            "name", LogMetrics.Log.SIZE.metricName());
    var attributes = Map.<String, Object>of("value", size);
    return new LogMetrics.Log.Gauge(new BeanObject(domainName, properties, attributes));
  }
}
