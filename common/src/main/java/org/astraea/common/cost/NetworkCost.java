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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.BrokerTopic;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.HasRate;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.Fetcher;

public abstract class NetworkCost implements HasClusterCost {

  static final AtomicBoolean isTesting = new AtomicBoolean(false);

  private final AtomicReference<ClusterInfo<Replica>> currentCluster = new AtomicReference<>();

  abstract ServerMetrics.Topic useMetric();

  @Override
  public ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
    // TODO: We need a reliable way to access the actual current cluster info. The following method
    //  try to compare the equality of cluster info and cluster bean in terms of replica set. But it
    //  didn't consider the data folder info. See the full discussion:
    //  https://github.com/skiptests/astraea/pull/1240#discussion_r1044487473
    var metricReplicas = clusterBean.replicas();
    var mismatchSet =
        clusterInfo.topicPartitionReplicas().stream()
            .filter(tpr -> !metricReplicas.contains(tpr))
            .collect(Collectors.toUnmodifiableSet());
    if (mismatchSet.isEmpty() || (isTesting.get() && currentCluster.get() == null))
      currentCluster.set(clusterInfo);
    if (currentCluster.get() == null)
      fail("Initial clusterInfo required, the following replicas are mismatch: " + mismatchSet);

    var dataRate = estimateRate(currentCluster.get(), clusterBean, useMetric());
    var brokerRate =
        clusterInfo
            .replicaStream()
            .filter(ReplicaInfo::isLeader)
            .filter(ReplicaInfo::isOnline)
            .collect(
                Collectors.groupingBy(
                    replica -> clusterInfo.node(replica.nodeInfo().id()),
                    Collectors.mapping(
                        replica -> notNull(dataRate.get(replica.topicPartition())),
                        Collectors.summingDouble(x -> x))));

    var summary = brokerRate.values().stream().mapToDouble(x -> x).summaryStatistics();
    if (summary.getMax() < 0)
      throw new IllegalStateException(
          "Corrupted max rate: " + summary.getMax() + ", brokers: " + brokerRate);
    if (summary.getMin() < 0)
      throw new IllegalStateException(
          "Corrupted min rate: " + summary.getMin() + ", brokers: " + brokerRate);
    if (summary.getMax() == 0) return () -> 0; // edge case to avoid divided by zero error
    double score = (summary.getMax() - summary.getMin()) / (summary.getMax());
    return () -> score;
  }

  @Override
  public Optional<Fetcher> fetcher() {
    // TODO: We need a reliable way to access the actual current cluster info. To do that we need to
    //  obtain the replica info, so we intentionally sample log size but never use it.
    //  https://github.com/skiptests/astraea/pull/1240#discussion_r1044487473
    return Fetcher.of(List.of(useMetric()::fetch, LogMetrics.Log.SIZE::fetch));
  }

  private Map<BrokerTopic, List<Replica>> mapLeaderAllocation(
      ClusterInfo<? extends Replica> clusterInfo) {
    return clusterInfo
        .replicaStream()
        .filter(ReplicaInfo::isOnline)
        .filter(ReplicaInfo::isLeader)
        .map(r -> Map.entry(BrokerTopic.of(r.nodeInfo().id(), r.topic()), r))
        .collect(
            Collectors.groupingBy(
                Map.Entry::getKey,
                Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableList())));
  }

  Map<TopicPartition, Long> estimateRate(
      ClusterInfo<? extends Replica> clusterInfo,
      ClusterBean clusterBean,
      ServerMetrics.Topic metric) {
    return mapLeaderAllocation(clusterInfo).entrySet().stream()
        .flatMap(
            e -> {
              var bt = e.getKey();
              var totalSize = e.getValue().stream().mapToLong(Replica::size).sum();
              var totalShare =
                  (double)
                      clusterBean
                          .brokerTopicMetrics(bt, ServerMetrics.Topic.Meter.class)
                          .filter(bean -> bean.metricsName().equals(metric.metricName()))
                          .max(Comparator.comparingLong(HasBeanObject::createdTimestamp))
                          .map(HasRate::fifteenMinuteRate)
                          .orElseThrow(
                              () ->
                                  new NoSufficientMetricsException(
                                      this, Duration.ofSeconds(1), "No metric for " + bt));
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
}
