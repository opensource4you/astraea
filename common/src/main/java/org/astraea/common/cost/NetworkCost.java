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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.DataRate;
import org.astraea.common.EnumInfo;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.BrokerTopic;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.cost.utils.ClusterInfoSensor;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.MetricSensor;
import org.astraea.common.metrics.platform.HostMetrics;

/**
 * This cost function calculate the load balance score in terms of network ingress or network
 * egress. The implementation is work under these assumptions:
 *
 * <ol>
 *   <li>The loading between each partition can be different. That is, there might be skewed load
 *       behavior between each partition.
 *   <li>The network ingress data rate of each partition is constant, it won't fluctuate over time.
 *   <li>The network egress data rate of each partition is constant, it won't fluctuate over time.
 *   <li>No consumer or consumer group attempts to subscribe or read a subset of partitions. It must
 *       subscribe to the whole topic.
 *   <li>This cost function relies on fifteen-minute rate metrics. To run two rebalance plans one by
 *       one, make sure there is a 15 minute interval between the generation of next plan and the
 *       execution of previous plan
 *   <li>This implementation assumes consumer won't fetch data from the closest replica. That is,
 *       every consumer fetches data from the leader(which is the default behavior of Kafka). For
 *       more detail about consumer rack awareness or how consumer can fetch data from the closest
 *       replica, see <a href="https://cwiki.apache.org/confluence/x/go_zBQ">KIP-392<a>.
 *   <li>NetworkCost implementation use broker-topic bandwidth rate and some other info to estimate
 *       the broker-topic-partition bandwidth rate. The implementation assume the broker-topic
 *       bandwidth is correct and steadily reflect the actual resource usage. This is generally true
 *       when the broker has reach its steady state, but to reach that state might takes awhile. And
 *       based on our observation this probably won't happen at the early broker start (see <a
 *       href="https://github.com/skiptests/astraea/issues/1641">Issue #1641</a>). We suggest use
 *       this cost with metrics from the servers in steady state.
 * </ol>
 */
public abstract class NetworkCost implements HasClusterCost {

  public static final String NETWORK_COST_ESTIMATION_METHOD = "network.cost.estimation.method";

  private final EstimationMethod estimationMethod;
  private final BandwidthType bandwidthType;
  private final Map<ClusterBean, CachedCalculation> calculationCache;
  private final ClusterInfoSensor clusterInfoSensor = new ClusterInfoSensor();

  NetworkCost(Configuration config, BandwidthType bandwidthType) {
    this.bandwidthType = bandwidthType;
    this.calculationCache = new ConcurrentHashMap<>();
    this.estimationMethod =
        config
            .string(NETWORK_COST_ESTIMATION_METHOD)
            .map(EstimationMethod::ofAlias)
            .orElse(EstimationMethod.BROKER_TOPIC_ONE_MINUTE_RATE);
  }

  void noMetricCheck(ClusterBean clusterBean) {
    var noMetricBrokers =
        clusterBean.all().entrySet().stream()
            .filter(e -> e.getValue().size() == 0)
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableList());
    if (!noMetricBrokers.isEmpty())
      throw new NoSufficientMetricsException(
          this,
          Duration.ofSeconds(1),
          "The following brokers have no metric available: " + noMetricBrokers);
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    noMetricCheck(clusterBean);

    // The partition load calculation takes considerable time with many partitions. cache the
    // calculation result to speed things up
    final var cachedCalculation =
        calculationCache.computeIfAbsent(clusterBean, CachedCalculation::new);

    // Evaluate the score of the balancer-tweaked cluster(clusterInfo)
    var brokerIngressRate =
        clusterInfo.brokers().stream()
            .collect(
                Collectors.toMap(
                    Broker::id,
                    broker ->
                        clusterInfo
                            .replicaStream(broker.id())
                            .mapToLong(
                                replica -> {
                                  // ingress might come from producer-send or follower-fetch.
                                  return ingress(cachedCalculation, replica.topicPartition());
                                })
                            .sum()));
    var brokerEgressRate =
        clusterInfo.brokers().stream()
            .collect(
                Collectors.toMap(
                    Broker::id,
                    broker ->
                        clusterInfo
                            .replicaStream(broker.id())
                            .mapToLong(
                                replica -> {
                                  // egress is composed of consumer-fetch and follower-fetch.
                                  // this implementation assumes no consumer rack awareness fetcher
                                  // enabled so all consumers fetch data from the leader only.
                                  return replica.isLeader()
                                      ? egress(cachedCalculation, replica.topicPartition())
                                          + ingress(cachedCalculation, replica.topicPartition())
                                              // Multiply by the number of follower replicas. This
                                              // number considers both online replicas and offline
                                              // replicas since an offline replica is probably a
                                              // transient behavior. So the offline state should get
                                              // resolved in the near future, we count it in
                                              // advance.
                                              * (clusterInfo
                                                      .replicas(replica.topicPartition())
                                                      .size()
                                                  - 1)
                                      : 0;
                                })
                            .sum()));
    // add the brokers having no replicas into map
    clusterInfo.brokers().stream()
        .filter(node -> !brokerIngressRate.containsKey(node.id()))
        .forEach(
            node -> {
              brokerIngressRate.put(node.id(), 0L);
              brokerEgressRate.put(node.id(), 0L);
            });

    // the rate we are measuring
    var brokerRate =
        (bandwidthType == BandwidthType.Ingress) ? brokerIngressRate : brokerEgressRate;

    var summary = brokerRate.values().stream().mapToDouble(x -> x).summaryStatistics();
    if (summary.getMax() < 0)
      throw new IllegalStateException(
          "Corrupted max rate: " + summary.getMax() + ", brokers: " + brokerRate);
    if (summary.getMin() < 0)
      throw new IllegalStateException(
          "Corrupted min rate: " + summary.getMin() + ", brokers: " + brokerRate);
    if (summary.getMax() == 0)
      return ClusterCost.of(
          0, () -> "network load zero"); // edge case to avoid divided by zero error

    // evaluate the max possible load of ingress & egress
    var maxIngress = brokerIngressRate.values().stream().mapToDouble(x -> x).sum();
    var maxEgress = brokerEgressRate.values().stream().mapToDouble(x -> x).sum();
    var maxRate = Math.max(maxIngress, maxEgress);
    // the score is measured as the ratio of targeting network throughput related to the maximum
    // ingress or egress throughput. See https://github.com/skiptests/astraea/issues/1285 for the
    // reason to do this.
    double score = (summary.getMax() - summary.getMin()) / (maxRate);

    return new NetworkClusterCost(score, brokerRate);
  }

  @Override
  public MetricSensor metricSensor() {
    // TODO: We need a reliable way to access the actual current cluster info. To do that we need to
    //  obtain the replica info, so we intentionally sample log size but never use it.
    //  https://github.com/skiptests/astraea/pull/1240#discussion_r1044487473
    return (client, clusterBean) ->
        Stream.of(
                List.of(HostMetrics.jvmMemory(client)),
                ServerMetrics.Topic.BYTES_IN_PER_SEC.fetch(client),
                ServerMetrics.Topic.BYTES_OUT_PER_SEC.fetch(client),
                LogMetrics.Log.SIZE.fetch(client),
                clusterInfoSensor.fetch(client, clusterBean))
            .flatMap(Collection::stream)
            .toList();
  }

  private Map<BrokerTopic, List<Replica>> mapLeaderAllocation(ClusterInfo clusterInfo) {
    return clusterInfo
        .replicaStream()
        .filter(Replica::isOnline)
        .filter(Replica::isLeader)
        .map(r -> Map.entry(BrokerTopic.of(r.broker().id(), r.topic()), r))
        .collect(
            Collectors.groupingBy(
                Map.Entry::getKey,
                Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableList())));
  }

  /**
   * Estimate the produce load for each partition. If a partition have no load metric in
   * ClusterBean, it will be considered as zero produce load.
   */
  Map<TopicPartition, Long> estimateRate(
      ClusterInfo clusterInfo, ClusterBean clusterBean, ServerMetrics.Topic metric) {
    return mapLeaderAllocation(clusterInfo).entrySet().stream()
        .flatMap(
            e -> {
              var bt = e.getKey();
              var totalSize = e.getValue().stream().mapToLong(Replica::size).sum();
              var totalShare =
                  (double)
                      clusterBean
                          .brokerTopicMetrics(bt, ServerMetrics.Topic.Meter.class)
                          .filter(bean -> bean.type().equals(metric))
                          .max(Comparator.comparingLong(HasBeanObject::createdTimestamp))
                          .map(
                              hasRate -> {
                                switch (estimationMethod) {
                                  case BROKER_TOPIC_ONE_MINUTE_RATE:
                                    return hasRate.oneMinuteRate();
                                  case BROKER_TOPIC_FIVE_MINUTE_RATE:
                                    return hasRate.fiveMinuteRate();
                                  case BROKER_TOPIC_FIFTEEN_MINUTE_RATE:
                                    return hasRate.fifteenMinuteRate();
                                  default:
                                    throw new IllegalStateException(
                                        "Unknown estimation method: " + estimationMethod);
                                }
                              })
                          // no load metric for this partition, treat as zero load
                          .orElse(0.0);
              if (Double.isNaN(totalShare) || totalShare < 0)
                throw new NoSufficientMetricsException(
                    this,
                    Duration.ofSeconds(1),
                    "Illegal load value " + totalShare + " for broker-topic: " + bt);
              var calculateShare =
                  (Function<Replica, Long>)
                      (replica) ->
                          totalSize > 0
                              ? (long) ((totalShare * replica.size()) / totalSize)
                              : totalSize == 0
                                  ? 0L
                                  : fail("Illegal replica with negative size: " + replica);

              return e.getValue().stream()
                  .map(r -> Map.entry(r.topicPartition(), calculateShare.apply(r)));
            })
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private long ingress(CachedCalculation calculation, TopicPartition topicPartition) {
    var value = calculation.partitionIngressRate.get(topicPartition);
    if (value == null) {
      // Maybe the user run into this bug: https://github.com/skiptests/astraea/issues/1388
      throw new NoSufficientMetricsException(
          this,
          Duration.ofSeconds(1),
          "Unable to resolve the network ingress rate of "
              + topicPartition
              + ". "
              + "If this issue persists for a while. Consider looking into the Astraea troubleshooting page.");
    }
    return value;
  }

  private long egress(CachedCalculation calculation, TopicPartition topicPartition) {
    var value = calculation.partitionEgressRate.get(topicPartition);
    if (value == null) {
      // Maybe the user run into this bug: https://github.com/skiptests/astraea/issues/1388
      throw new NoSufficientMetricsException(
          this,
          Duration.ofSeconds(1),
          "Unable to resolve the network egress rate of "
              + topicPartition
              + ". "
              + "If this issue persists for a while. Consider looking into the Astraea troubleshooting page.");
    }
    return value;
  }

  private <T> T fail(String reason) {
    throw new NoSufficientMetricsException(this, Duration.ofSeconds(1), reason);
  }

  @Override
  public abstract String toString();

  enum BandwidthType implements EnumInfo {
    Ingress,
    Egress;

    static BandwidthType ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(BandwidthType.class, alias);
    }

    @Override
    public String alias() {
      return name();
    }

    @Override
    public String toString() {
      return alias();
    }
  }

  private class CachedCalculation {
    private final Map<TopicPartition, Long> partitionIngressRate;
    private final Map<TopicPartition, Long> partitionEgressRate;

    private CachedCalculation(ClusterBean sourceMetric) {
      ClusterInfo metricViewCluster;
      try {
        metricViewCluster = ClusterInfoSensor.metricViewCluster(sourceMetric);
      } catch (IllegalStateException e) {
        // ClusterInfoSensor#metricViewCluster throws IllegalStateException when the given metrics
        // contain conflict information(for example there is a partition at one broker, but its size
        // metrics is not present in the given metrics. This might happen due to metric truncation.
        throw new NoSufficientMetricsException(
            NetworkCost.this, Duration.ofSeconds(1), "There ClusterBean is not ready yet", e);
      }
      this.partitionIngressRate =
          estimateRate(metricViewCluster, sourceMetric, ServerMetrics.Topic.BYTES_IN_PER_SEC);
      this.partitionEgressRate =
          estimateRate(metricViewCluster, sourceMetric, ServerMetrics.Topic.BYTES_OUT_PER_SEC);
    }
  }

  static class NetworkClusterCost implements ClusterCost {
    final double score;
    final Map<Integer, Long> brokerRate;

    NetworkClusterCost(double score, Map<Integer, Long> brokerRate) {
      this.score = score;
      this.brokerRate = brokerRate;
    }

    public double value() {
      return score;
    }

    @Override
    public String toString() {
      return brokerRate.values().stream()
          .map(DataRate.Byte::of)
          .map(DataRate::toString)
          .collect(Collectors.joining(", ", "{", "}"));
    }
  }
}
