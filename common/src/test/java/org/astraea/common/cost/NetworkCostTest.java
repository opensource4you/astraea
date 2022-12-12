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
import java.util.function.Function;
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

  @Test
  void testInitialClusterInfo() {
    NetworkCost.isTesting.set(false);
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
                    bandwidth(ServerMetrics.Topic.BYTES_IN_PER_SEC, "Pipeline", 0))));
    var noLog =
        ClusterBean.of(
            Map.of(1, List.of(bandwidth(ServerMetrics.Topic.BYTES_IN_PER_SEC, "Pipeline", 0))));

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
    NetworkCost.isTesting.set(true);
    testEstimatedRate(ServerMetrics.Topic.BYTES_IN_PER_SEC);
  }

  @Test
  void testEstimatedEgressRate() {
    NetworkCost.isTesting.set(true);
    testEstimatedRate(ServerMetrics.Topic.BYTES_OUT_PER_SEC);
  }

  void testEstimatedRate(ServerMetrics.Topic metric) {
    var t = new HandWrittenTestCase(metric);
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
    var in = ServerMetrics.Topic.BYTES_IN_PER_SEC;
    var out = ServerMetrics.Topic.BYTES_OUT_PER_SEC;
    return Stream.of(
        Arguments.of(new NetworkIngressCost(), new HandWrittenTestCase(in)),
        Arguments.of(new NetworkIngressCost(), new LargeTestCase(in, 2, 100, 0xfeedbabe)),
        Arguments.of(new NetworkIngressCost(), new LargeTestCase(in, 3, 100, 0xabcdef01)),
        Arguments.of(new NetworkIngressCost(), new LargeTestCase(in, 10, 200, 0xcafebabe)),
        Arguments.of(new NetworkIngressCost(), new LargeTestCase(in, 15, 300, 0xfee1dead)),
        Arguments.of(new NetworkEgressCost(), new HandWrittenTestCase(out)),
        Arguments.of(new NetworkEgressCost(), new LargeTestCase(out, 5, 100, 0xfa11fa11)),
        Arguments.of(new NetworkEgressCost(), new LargeTestCase(out, 6, 100, 0xf001f001)),
        Arguments.of(new NetworkEgressCost(), new LargeTestCase(out, 8, 200, 0xba1aba1a)),
        Arguments.of(new NetworkEgressCost(), new LargeTestCase(out, 14, 300, 0xdd0000bb)));
  }

  @ParameterizedTest
  @MethodSource("testcases")
  @DisplayName("Run with Balancer")
  void testOptimization(HasClusterCost costFunction, TestCase testcase) {
    NetworkCost.isTesting.set(true);
    var newPlan =
        Balancer.Official.Greedy.create(
                AlgorithmConfig.builder()
                    .clusterCost(costFunction)
                    .metricSource(testcase::clusterBean)
                    .build())
            .offer(testcase.clusterInfo(), Duration.ofSeconds(1));

    Assertions.assertTrue(newPlan.isPresent());
    System.out.println("Initial cost: " + newPlan.get().initialClusterCost().value());
    System.out.println("New cost: " + newPlan.get().proposalClusterCost().value());
    Assertions.assertTrue(
        newPlan.get().initialClusterCost().value() > newPlan.get().proposalClusterCost().value());
  }

  @Test
  void testCompositeOptimization() {
    NetworkCost.isTesting.set(true);
    var testCase =
        new LargeTestCase(
            ServerMetrics.Topic.BYTES_IN_PER_SEC,
            ServerMetrics.Topic.BYTES_OUT_PER_SEC,
            10,
            300,
            0xfeed);
    var costFunction =
        HasClusterCost.of(
            Map.of(
                new NetworkIngressCost(), 1.0,
                new NetworkEgressCost(), 1.0));
    testOptimization(costFunction, testCase);
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
    private HandWrittenTestCase(ServerMetrics.Topic metric) {
      this.clusterBean =
          ClusterBean.of(
              Map.of(
                  1,
                  expectedBrokerTopicBandwidth.get(1).entrySet().stream()
                      .map(e -> bandwidth(metric, e.getKey(), e.getValue()))
                      .collect(Collectors.toUnmodifiableList()),
                  2,
                  expectedBrokerTopicBandwidth.get(2).entrySet().stream()
                      .map(e -> bandwidth(metric, e.getKey(), e.getValue()))
                      .collect(Collectors.toUnmodifiableList()),
                  3,
                  expectedBrokerTopicBandwidth.get(3).entrySet().stream()
                      .map(e -> bandwidth(metric, e.getKey(), e.getValue()))
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

    public LargeTestCase(ServerMetrics.Topic metric, int brokers, int partitions, int seed) {
      this(metric, null, brokers, partitions, seed);
    }

    public LargeTestCase(
        ServerMetrics.Topic metric0,
        ServerMetrics.Topic metric1,
        int brokers,
        int partitions,
        int seed) {
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
                                      metric0,
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
                                      metric1,
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
