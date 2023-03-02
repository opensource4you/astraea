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
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.DataRate;
import org.astraea.common.EnumInfo;
import org.astraea.common.admin.BrokerTopic;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.cost.utils.ClusterInfoSensor;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.HasRate;
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
 * </ol>
 */
public abstract class NetworkCost implements HasClusterCost {

  private final BandwidthType bandwidthType;
  private final ClusterInfoSensor clusterInfoSensor = new ClusterInfoSensor();

  NetworkCost(BandwidthType bandwidthType) {
    this.bandwidthType = bandwidthType;
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
    final var metricViewCluster = ClusterInfoSensor.metricViewCluster(clusterBean);

    // Use the real cluster(metricViewCluster) and real metrics to derive the data rate of each
    // partition
    var ingressRate =
        estimateRate(metricViewCluster, clusterBean, ServerMetrics.Topic.BYTES_IN_PER_SEC);
    var egressRate =
        estimateRate(metricViewCluster, clusterBean, ServerMetrics.Topic.BYTES_OUT_PER_SEC);
    // Evaluate the score of the balancer-tweaked cluster(clusterInfo)
    var brokerIngressRate =
        clusterInfo
            .replicaStream()
            .filter(Replica::isOnline)
            .collect(
                Collectors.groupingBy(
                    replica -> clusterInfo.node(replica.nodeInfo().id()),
                    Collectors.mapping(
                        replica -> {
                          // ingress might come from producer-send or follower-fetch.
                          return notNull(ingressRate.get(replica.topicPartition()));
                        },
                        Collectors.summingDouble(x -> x))));
    var brokerEgressRate =
        clusterInfo
            .replicaStream()
            .filter(Replica::isOnline)
            .collect(
                Collectors.groupingBy(
                    replica -> clusterInfo.node(replica.nodeInfo().id()),
                    Collectors.mapping(
                        replica -> {
                          // egress is composed of consumer-fetch and follower-fetch.
                          // this implementation assumes no consumer rack awareness fetcher
                          // enabled so all consumers fetch data from the leader only.
                          return replica.isLeader()
                              ? notNull(egressRate.get(replica.topicPartition()))
                                  + notNull(ingressRate.get(replica.topicPartition()))
                                      // Multiply by the number of follower replicas. This
                                      // number considers both online replicas and offline
                                      // replicas since an offline replica is probably a
                                      // transient behavior. So the offline state should get
                                      // resolved in the near future, we count it in advance.
                                      * (clusterInfo.replicas(replica.topicPartition()).size() - 1)
                              : 0;
                        },
                        Collectors.summingDouble(x -> x))));
    // add the brokers having no replicas into map
    clusterInfo.nodes().stream()
        .filter(node -> !brokerIngressRate.containsKey(node))
        .forEach(
            node -> {
              brokerIngressRate.put(node, 0.0);
              brokerEgressRate.put(node, 0.0);
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
  public Optional<MetricSensor> metricSensor() {
    // TODO: We need a reliable way to access the actual current cluster info. To do that we need to
    //  obtain the replica info, so we intentionally sample log size but never use it.
    //  https://github.com/skiptests/astraea/pull/1240#discussion_r1044487473
    return Optional.of(
        (client, clusterBean) ->
            Stream.of(
                    List.of(HostMetrics.jvmMemory(client)),
                    ServerMetrics.Topic.BYTES_IN_PER_SEC.fetch(client),
                    ServerMetrics.Topic.BYTES_OUT_PER_SEC.fetch(client),
                    LogMetrics.Log.SIZE.fetch(client),
                    clusterInfoSensor.fetch(client, clusterBean))
                .flatMap(Collection::stream)
                .collect(Collectors.toUnmodifiableList()));
  }

  private Map<BrokerTopic, List<Replica>> mapLeaderAllocation(ClusterInfo clusterInfo) {
    return clusterInfo
        .replicaStream()
        .filter(Replica::isOnline)
        .filter(Replica::isLeader)
        .map(r -> Map.entry(BrokerTopic.of(r.nodeInfo().id(), r.topic()), r))
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
                          .map(HasRate::fifteenMinuteRate)
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

  private <T> T notNull(T value) {
    if (value == null)
      throw new NoSufficientMetricsException(this, Duration.ofSeconds(1), "No metric");
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

  static class NetworkClusterCost implements ClusterCost {
    final double score;
    final Map<NodeInfo, Double> brokerRate;

    NetworkClusterCost(double score, Map<NodeInfo, Double> brokerRate) {
      this.score = score;
      this.brokerRate = brokerRate;
    }

    public double value() {
      return score;
    }

    @Override
    public String toString() {
      return brokerRate.values().stream()
          .map(x -> DataRate.Byte.of(x.longValue()).perSecond())
          .map(DataRate::toString)
          .collect(Collectors.joining(", ", "{", "}"));
    }
  }
}
