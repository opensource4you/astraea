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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.DataRate;
import org.astraea.common.Utils;
import org.astraea.common.admin.BrokerTopic;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoBuilder;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.tweakers.ShuffleTweaker;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.MetricFactory;
import org.astraea.common.metrics.MetricSeriesBuilder;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.MetricCollector;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class NetworkCostTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void close() {
    SERVICE.close();
  }

  @Test
  void testEstimatedIngressRate() {
    var t = new LargeTestCase(3, 10, 0);
    var expected =
        Map.ofEntries(
            Map.entry(TopicPartition.of("Pipeline-9"), 100000000L),
            Map.entry(TopicPartition.of("Pipeline-8"), 125000000L),
            Map.entry(TopicPartition.of("Pipeline-7"), 220000000L),
            Map.entry(TopicPartition.of("Pipeline-6"), 12000000L),
            Map.entry(TopicPartition.of("Pipeline-5"), 27000000L),
            Map.entry(TopicPartition.of("Pipeline-4"), 4000000L),
            Map.entry(TopicPartition.of("Pipeline-3"), 11000000L),
            Map.entry(TopicPartition.of("Pipeline-2"), 9000000L),
            Map.entry(TopicPartition.of("Pipeline-1"), 47000000L),
            Map.entry(TopicPartition.of("Pipeline-0"), 144000000L));
    Assertions.assertEquals(
        expected,
        new NetworkIngressCost()
            .estimateRate(t.clusterInfo(), t.clusterBean(), ServerMetrics.Topic.BYTES_IN_PER_SEC));
  }

  @Test
  void testEstimatedEgressRate() {
    var t = new LargeTestCase(3, 10, 0);
    var expected =
        Map.ofEntries(
            Map.entry(TopicPartition.of("Pipeline-9"), 316479400L),
            Map.entry(TopicPartition.of("Pipeline-8"), 222049689L),
            Map.entry(TopicPartition.of("Pipeline-7"), 668929889L),
            Map.entry(TopicPartition.of("Pipeline-6"), 37977528L),
            Map.entry(TopicPartition.of("Pipeline-5"), 47962732L),
            Map.entry(TopicPartition.of("Pipeline-4"), 12162361L),
            Map.entry(TopicPartition.of("Pipeline-3"), 34812734L),
            Map.entry(TopicPartition.of("Pipeline-2"), 15987577L),
            Map.entry(TopicPartition.of("Pipeline-1"), 142907749L),
            Map.entry(TopicPartition.of("Pipeline-0"), 455730337L));
    Assertions.assertEquals(
        expected,
        new NetworkEgressCost()
            .estimateRate(t.clusterInfo(), t.clusterBean(), ServerMetrics.Topic.BYTES_OUT_PER_SEC));
  }

  static Stream<Arguments> testcases() {
    return Stream.of(
        Arguments.of(new NetworkIngressCost(), new LargeTestCase(2, 100, 0xfeedbabe)),
        Arguments.of(new NetworkIngressCost(), new LargeTestCase(3, 100, 0xabcdef01)),
        Arguments.of(new NetworkIngressCost(), new LargeTestCase(10, 200, 0xcafebabe)),
        Arguments.of(new NetworkIngressCost(), new LargeTestCase(15, 300, 0xfee1dead)),
        Arguments.of(new NetworkEgressCost(), new LargeTestCase(5, 100, 0xfa11fa11)),
        Arguments.of(new NetworkEgressCost(), new LargeTestCase(6, 100, 0xf001f001)),
        Arguments.of(new NetworkEgressCost(), new LargeTestCase(8, 200, 0xba1aba1a)),
        Arguments.of(new NetworkEgressCost(), new LargeTestCase(14, 300, 0xdd0000bb)));
  }

  @ParameterizedTest
  @MethodSource("testcases")
  @DisplayName("Run with Balancer")
  void testOptimization(HasClusterCost costFunction, TestCase testcase) {
    var newPlan =
        Balancer.Official.Greedy.create(Configuration.EMPTY)
            .offer(
                AlgorithmConfig.builder()
                    .clusterInfo(testcase.clusterInfo())
                    .clusterBean(testcase.clusterBean())
                    .timeout(Duration.ofSeconds(1))
                    .clusterCost(costFunction)
                    .build());

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
                new NetworkIngressCost(), 1.0,
                new NetworkEgressCost(), 1.0));
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
                        MetricFactory.ofPartitionMetric("Rain", 0, 2),
                        MetricFactory.ofPartitionMetric("Drop", 0, 0),
                        LogMetrics.Log.SIZE.builder().topic("Rain").partition(0).logSize(1).build(),
                        LogMetrics.Log.SIZE.builder().topic("Drop").partition(0).logSize(1).build(),
                        bandwidth(ServerMetrics.Topic.BYTES_IN_PER_SEC, "Rain", 100),
                        bandwidth(ServerMetrics.Topic.BYTES_OUT_PER_SEC, "Rain", 300)),
                2,
                    List.of(
                        MetricFactory.ofPartitionMetric("Rain", 0, 0),
                        LogMetrics.Log.SIZE.builder().topic("Rain").partition(0).logSize(1).build(),
                        noise(5566)),
                3,
                    List.of(
                        MetricFactory.ofPartitionMetric("Drop", 0, 2),
                        LogMetrics.Log.SIZE.builder().topic("Drop").partition(0).logSize(1).build(),
                        bandwidth(ServerMetrics.Topic.BYTES_IN_PER_SEC, "Drop", 80),
                        bandwidth(ServerMetrics.Topic.BYTES_OUT_PER_SEC, "Drop", 800))));

    double error = 1e-9;
    Function<Double, Predicate<Double>> around =
        (expected) -> (value) -> (expected + error) >= value && value >= (expected - error);

    double expectedIngress1 = 100 + 80;
    double expectedIngress2 = 100;
    double expectedIngress3 = 80;
    double ingressSum = expectedIngress1 + expectedIngress2 + expectedIngress3;
    double expectedEgress1 = 300 + 100;
    double expectedEgress2 = 0;
    double expectedEgress3 = 800 + 80;
    double egressSum = expectedEgress1 + expectedEgress2 + expectedEgress3;
    double expectedIngressScore =
        (expectedIngress1 - expectedIngress3) / Math.max(ingressSum, egressSum);
    double expectedEgressScore =
        (expectedEgress3 - expectedEgress2) / Math.max(ingressSum, egressSum);
    double ingressScore = new NetworkIngressCost().clusterCost(cluster, beans).value();
    double egressScore = new NetworkEgressCost().clusterCost(cluster, beans).value();
    Assertions.assertTrue(
        around.apply(expectedIngressScore).test(ingressScore),
        "Ingress score should be " + expectedIngressScore + " but it is " + ingressScore);
    Assertions.assertTrue(
        around.apply(expectedEgressScore).test(egressScore),
        "Egress score should be " + expectedEgressScore + " but it is " + egressScore);
    Assertions.assertTrue(
        egressScore > ingressScore,
        "Egress is experience higher imbalance issues, its score should be higher");
  }

  @Test
  void testZeroBandwidth() {
    // if a partition has no produce/fetch occur, it won't have a BytesInPerSec or BytesOutPerSec
    // metric entry on the remote Kafka MBeanServer. To work around this we should treat them as
    // zero partition ingress/egress instead of complaining no metric available.
    var cluster =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(Map.of(1, Set.of("/folder0", "/folder1")))
            .addFolders(Map.of(2, Set.of("/folder0", "/folder1")))
            .addFolders(Map.of(3, Set.of("/folder0", "/folder1")))
            .addTopic("Pipeline", 1, (short) 3)
            .build();
    var beans =
        ClusterBean.of(
            Map.of(
                1,
                    List.of(
                        MetricFactory.ofPartitionMetric("Pipeline", 0, 2),
                        LogMetrics.Log.SIZE
                            .builder()
                            .topic("Pipeline")
                            .partition(0)
                            .logSize(0)
                            .build(),
                        noise(5566)),
                2,
                    List.of(
                        MetricFactory.ofPartitionMetric("Pipeline", 0, 0),
                        LogMetrics.Log.SIZE
                            .builder()
                            .topic("Pipeline")
                            .partition(0)
                            .logSize(0)
                            .build(),
                        noise(5566)),
                3,
                    List.of(
                        MetricFactory.ofPartitionMetric("Pipeline", 0, 0),
                        LogMetrics.Log.SIZE
                            .builder()
                            .topic("Pipeline")
                            .partition(0)
                            .logSize(0)
                            .build(),
                        noise(5566))));
    Assertions.assertDoesNotThrow(
        () -> new NetworkIngressCost().clusterCost(cluster, beans),
        "Metric sampled but no load value, treat as zero load");
    Assertions.assertDoesNotThrow(
        () -> new NetworkEgressCost().clusterCost(cluster, beans),
        "Metric sampled but no load value, treat as zero load");
    Assertions.assertEquals(0, new NetworkIngressCost().clusterCost(cluster, beans).value());
    Assertions.assertEquals(0, new NetworkEgressCost().clusterCost(cluster, beans).value());

    Assertions.assertThrows(
        NoSufficientMetricsException.class,
        () ->
            new NetworkIngressCost()
                .clusterCost(
                    cluster, ClusterBean.of(Map.of(1, List.of(), 2, List.of(), 3, List.of()))),
        "Should raise a exception since we don't know if first sample is performed or not");
    Assertions.assertThrows(
        NoSufficientMetricsException.class,
        () ->
            new NetworkEgressCost()
                .clusterCost(
                    cluster, ClusterBean.of(Map.of(1, List.of(), 2, List.of(), 3, List.of()))),
        "Should raise a exception since we don't know if first sample is performed or not");
  }

  @ParameterizedTest
  @ValueSource(ints = {0xfee1dead, 1, 100, 500, -5566, 0xcafebabe, 0x5566, 0x996, 0xABCDEF})
  @Disabled
  void testExpectedImprovement(int seed) {
    var testCase = new LargeTestCase(6, 100, seed);
    var clusterInfo = testCase.clusterInfo();
    var clusterBean = testCase.clusterBean();
    var smallShuffle =
        new ShuffleTweaker(() -> ThreadLocalRandom.current().nextInt(1, 6), (x) -> true);
    var largeShuffle =
        new ShuffleTweaker(() -> ThreadLocalRandom.current().nextInt(1, 31), (x) -> true);
    var costFunction =
        HasClusterCost.of(Map.of(new NetworkIngressCost(), 1.0, new NetworkEgressCost(), 1.0));
    var originalCost = costFunction.clusterCost(clusterInfo, clusterBean);

    Function<ShuffleTweaker, Double> experiment =
        (tweaker) -> {
          return IntStream.range(0, 10)
              .mapToDouble(
                  (ignore) -> {
                    var end = System.currentTimeMillis() + Duration.ofMillis(1000).toMillis();
                    var timeUp = (Supplier<Boolean>) () -> (System.currentTimeMillis() > end);
                    var counting = 0;
                    var next = clusterInfo;
                    while (!timeUp.get()) {
                      next =
                          tweaker
                              .generate(next)
                              .parallel()
                              .limit(30)
                              .takeWhile(i -> !timeUp.get())
                              .map(
                                  cluster ->
                                      Map.entry(
                                          cluster,
                                          costFunction.clusterCost(cluster, clusterBean).value()))
                              .filter(e -> originalCost.value() > e.getValue())
                              .min(Map.Entry.comparingByValue())
                              .map(Map.Entry::getKey)
                              .orElse(next);
                      counting++;
                    }
                    var a = originalCost.value();
                    var b = costFunction.clusterCost(next, clusterBean).value();
                    System.out.println(counting);
                    return a - b;
                  })
              .average()
              .orElseThrow();
        };

    long s0 = System.currentTimeMillis();
    double small = experiment.apply(smallShuffle);
    long s1 = System.currentTimeMillis();
    double large = experiment.apply(largeShuffle);
    long s2 = System.currentTimeMillis();

    double largeTime = (s2 - s1) / 1000.0;
    double smallTime = (s1 - s0) / 1000.0;
    System.out.println("[Use seed " + seed + "]");
    System.out.println(small + " (takes " + largeTime + "s)");
    System.out.println(large + " (takes " + smallTime + "s)");
    System.out.println("Small step is better: " + (small > large));
    System.out.println();
  }

  @Test
  void testZeroReplicaBroker() {
    var testcase = new LargeTestCase(1, 1, 0);
    var beans = testcase.clusterBean();
    var cluster = testcase.clusterInfo();
    var scaledCluster =
        ClusterInfoBuilder.builder(cluster)
            .addNode(Set.of(4321))
            .addFolders(Map.of(4321, Set.of("/folder")))
            .build();
    var node = scaledCluster.node(4321);

    var costI =
        (NetworkCost.NetworkClusterCost) new NetworkIngressCost().clusterCost(scaledCluster, beans);
    Assertions.assertEquals(2, costI.brokerRate.size());
    Assertions.assertEquals(0L, costI.brokerRate.get(node.id()));
    var costE =
        (NetworkCost.NetworkClusterCost) new NetworkEgressCost().clusterCost(scaledCluster, beans);
    Assertions.assertEquals(2, costE.brokerRate.size());
    Assertions.assertEquals(0L, costE.brokerRate.get(node.id()));
  }

  @Test
  void testNoMetricCheck() {
    var ingressCost = new NetworkIngressCost();
    var collectorBuilder = MetricCollector.local().interval(Duration.ofMillis(100));
    ingressCost.metricSensor().ifPresent(collectorBuilder::addMetricSensor);
    try (var collector = collectorBuilder.build()) {

      // sample metrics for a while.
      Utils.sleep(Duration.ofMillis(500));

      var emptyCluster = ClusterInfoBuilder.builder().addNode(Set.of(1, 2, 3)).build();
      Assertions.assertDoesNotThrow(
          () -> ingressCost.clusterCost(emptyCluster, collector.clusterBean()),
          "Should not raise an exception");
    }
  }

  @Test
  void testPartitionCost() {
    var ingressCost = new NetworkIngressCost();

    // create topic `test` and 9 partitions, the size of each partition is ( partition id +1 ) *
    // 10KB
    var clusterInfo =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(
                Map.of(
                    1,
                    Set.of("/folder0", "/folder1"),
                    2,
                    Set.of("/folder0", "/folder1"),
                    3,
                    Set.of("/folder0", "/folder1")))
            .addTopic(
                "test",
                9,
                (short) 1,
                replica ->
                    Replica.builder(replica)
                        .size(
                            (long)
                                ((replica.partition() + 1)
                                    * DataRate.KB.of(10).perSecond().byteRate()))
                        .build())
            .build();

    var clusterBean =
        ClusterBean.of(
            Map.of(
                1,
                List.of(
                    bandwidth(ServerMetrics.Topic.BYTES_IN_PER_SEC, "test", (1 + 4 + 7) * 10000)),
                2,
                List.of(
                    bandwidth(ServerMetrics.Topic.BYTES_IN_PER_SEC, "test", (2 + 5 + 8) * 10000)),
                3,
                List.of(
                    bandwidth(ServerMetrics.Topic.BYTES_IN_PER_SEC, "test", (3 + 6 + 9) * 10000))));

    var ingressPartitionCost = ingressCost.partitionCost(clusterInfo, clusterBean).value();

    Assertions.assertEquals(ingressPartitionCost.get(TopicPartition.of("test-0")), (double) 1 / 12);
    Assertions.assertEquals(ingressPartitionCost.get(TopicPartition.of("test-1")), (double) 2 / 15);
    Assertions.assertEquals(ingressPartitionCost.get(TopicPartition.of("test-2")), (double) 3 / 18);
    Assertions.assertEquals(ingressPartitionCost.get(TopicPartition.of("test-3")), (double) 4 / 12);
    Assertions.assertEquals(ingressPartitionCost.get(TopicPartition.of("test-4")), (double) 5 / 15);
    Assertions.assertEquals(ingressPartitionCost.get(TopicPartition.of("test-5")), (double) 6 / 18);
    Assertions.assertEquals(ingressPartitionCost.get(TopicPartition.of("test-6")), (double) 7 / 12);
    Assertions.assertEquals(ingressPartitionCost.get(TopicPartition.of("test-7")), (double) 8 / 15);
    Assertions.assertEquals(ingressPartitionCost.get(TopicPartition.of("test-8")), (double) 9 / 18);
  }

  interface TestCase {

    ClusterInfo clusterInfo();

    ClusterBean clusterBean();
  }

  /** A large cluster */
  private static class LargeTestCase implements TestCase {

    private final ClusterInfo clusterInfo;
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
          MetricSeriesBuilder.builder()
              .cluster(clusterInfo)
              .timeRange(LocalDateTime.now(), Duration.ZERO)
              .seriesByBrokerTopic(
                  (time, broker, topic) ->
                      ServerMetrics.Topic.BYTES_IN_PER_SEC
                          .builder()
                          .topic(topic)
                          .time(time.toEpochSecond(ZoneOffset.UTC))
                          .fifteenMinuteRate(
                              clusterInfo
                                  .replicaStream(BrokerTopic.of(broker, topic))
                                  .filter(Replica::isLeader)
                                  .filter(Replica::isOnline)
                                  .mapToDouble(r -> rate.get(r.topicPartition()))
                                  .sum())
                          .build())
              .seriesByBrokerTopic(
                  (time, broker, topic) ->
                      ServerMetrics.Topic.BYTES_OUT_PER_SEC
                          .builder()
                          .topic(topic)
                          .time(time.toEpochSecond(ZoneOffset.UTC))
                          .fifteenMinuteRate(
                              clusterInfo
                                  .replicaStream(BrokerTopic.of(broker, topic))
                                  .filter(Replica::isLeader)
                                  .filter(Replica::isOnline)
                                  .mapToDouble(
                                      r ->
                                          rate.get(r.topicPartition())
                                              * consumerFanout.get(r.topicPartition()))
                                  .sum())
                          .build())
              .seriesByBroker(
                  (time, broker) ->
                      IntStream.range(0, 10)
                          .mapToObj(
                              i ->
                                  ServerMetrics.Topic.TOTAL_FETCH_REQUESTS_PER_SEC
                                      .builder()
                                      .topic("Noise_" + i)
                                      .time(time.toEpochSecond(ZoneOffset.UTC))
                                      .build()))
              .seriesByBrokerReplica(
                  (time, broker, replica) ->
                      LogMetrics.Log.SIZE
                          .builder()
                          .topic(replica.topic())
                          .partition(replica.partition())
                          .logSize(replica.size())
                          .build())
              .seriesByBrokerReplica(
                  (time, broker, replica) ->
                      MetricFactory.ofPartitionMetric(
                          replica.topic(), replica.partition(), replica.isLeader() ? 1 : 0))
              .build();
    }

    @Override
    public ClusterInfo clusterInfo() {
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
            "type", "Log",
            "topic", topicPartition.topic(),
            "partition", String.valueOf(topicPartition.partition()),
            "name", LogMetrics.Log.SIZE.metricName());
    var attributes = Map.<String, Object>of("Value", size);
    return new LogMetrics.Log.Gauge(new BeanObject(domainName, properties, attributes));
  }
}
