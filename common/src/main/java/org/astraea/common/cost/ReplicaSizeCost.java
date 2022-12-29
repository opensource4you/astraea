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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.DataSize;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.Sensor;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.collector.Fetcher;
import org.astraea.common.metrics.collector.MetricSensor;
import org.astraea.common.metrics.stats.Avg;

/**
 * PartitionCost: more replica log size -> higher partition score BrokerCost: more replica log size
 * in broker -> higher broker score ClusterCost: The more unbalanced the replica log size among
 * brokers -> higher cluster score MoveCost: more replicas log size migrate -> higher cost
 */
public class ReplicaSizeCost
    implements HasMoveCost, HasBrokerCost, HasClusterCost, HasPartitionCost {
  private final Dispersion dispersion = Dispersion.cov();
  private static final String LOG_SIZE_EXP_WEIGHT_BY_TIME_KEY = "log_size_exp_weight_by_time";
  final Map<TopicPartitionReplica, Sensor<Double>> sensors = new HashMap<>();

  /**
   * @return the metrics getters. Those getters are used to fetch mbeans.
   */
  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(LogMetrics.Log.SIZE::fetch);
  }

  @Override
  public Collection<MetricSensor> sensors() {
    return List.of(
        (identity, beans) ->
            Map.of(
                identity,
                beans.stream()
                    .filter(b -> b instanceof LogMetrics.Log.Gauge)
                    .map(b -> (LogMetrics.Log.Gauge) b)
                    .filter(g -> g.type() == LogMetrics.Log.SIZE)
                    .map(
                        g -> {
                          var tpr = TopicPartitionReplica.of(g.topic(), g.partition(), identity);
                          var sensor =
                              sensors.computeIfAbsent(
                                  tpr,
                                  ignored ->
                                      Sensor.builder()
                                          .addStat(
                                              LOG_SIZE_EXP_WEIGHT_BY_TIME_KEY,
                                              Avg.expWeightByTime(Duration.ofSeconds(1)))
                                          .build());
                          sensor.record(g.value().doubleValue());
                          return (SizeStatisticalBean)
                              () ->
                                  new BeanObject(
                                      g.beanObject().domainName(),
                                      g.beanObject().properties(),
                                      Map.of(
                                          HasGauge.VALUE_KEY,
                                          sensor.measure(LOG_SIZE_EXP_WEIGHT_BY_TIME_KEY)),
                                      System.currentTimeMillis());
                        })
                    .collect(Collectors.toList())));
  }

  public interface SizeStatisticalBean extends HasGauge<Double> {}

  @Override
  public MoveCost moveCost(
      ClusterInfo<Replica> before, ClusterInfo<Replica> after, ClusterBean clusterBean) {
    return MoveCost.movedReplicaSize(
        Stream.concat(before.nodes().stream(), after.nodes().stream())
            .map(NodeInfo::id)
            .distinct()
            .parallel()
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    id ->
                        DataSize.Byte.of(
                            after.replicaStream(id).mapToLong(r -> r.size()).sum()
                                - before.replicaStream(id).mapToLong(r -> r.size()).sum()))));
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a BrokerCost contains the used space for each broker
   */
  @Override
  public BrokerCost brokerCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var logSize =
        clusterBean.replicas().stream()
            .map(
                p ->
                    clusterBean
                        .replicaMetrics(p, SizeStatisticalBean.class)
                        .max(Comparator.comparing(HasBeanObject::createdTimestamp))
                        .orElseThrow(
                            () ->
                                new NoSufficientMetricsException(
                                    this, Duration.ofSeconds(1), "No metric for " + p)))
            .collect(
                Collectors.groupingBy(
                    bean ->
                        TopicPartition.of(
                            bean.topicIndex().get(), bean.partitionIndex().get().partition()),
                    Collectors.mapping(HasGauge::value, Collectors.toList())));
    var result =
        clusterInfo.topicPartitionReplicas().stream()
            .collect(
                Collectors.groupingBy(
                    TopicPartitionReplica::brokerId,
                    Collectors.mapping(
                        tpr ->
                            statistReplicaSizeCount(
                                    clusterInfo
                                        .replicaLeader(tpr.topicPartition())
                                        .map(ReplicaInfo::topicPartitionReplica)
                                        .orElse(tpr),
                                    clusterBean)
                                .orElse(
                                    maxPartitionSize(
                                        tpr.topicPartition(),
                                        logSize.getOrDefault(tpr.topicPartition(), List.of()))),
                        Collectors.summingDouble(x -> x))));
    return () -> result;
  }

  double maxPartitionSize(TopicPartition tp, List<Double> size) {
    if (size.isEmpty())
      throw new NoSufficientMetricsException(this, Duration.ofSeconds(1), "No metric for " + tp);
    checkSizeDiff(tp, size, 0.2);
    return size.stream()
        .max(Comparator.comparing(Function.identity()))
        .orElseThrow(
            () ->
                new NoSufficientMetricsException(
                    this, Duration.ofSeconds(1), "No metric for " + tp));
  }

  /**
   * Check whether the partition log size difference of all replicas is within a certain percentage
   *
   * @param tp topicPartition
   * @param size log size list of partition tp
   * @param percentage the percentage of the maximum acceptable partition data volume difference
   *     between replicas
   */
  private void checkSizeDiff(TopicPartition tp, List<Double> size, double percentage) {
    var max = size.stream().max(Comparator.comparing(Function.identity())).get();
    var min = size.stream().min(Comparator.comparing(Function.identity())).get();
    if ((max - min) / min > percentage)
      throw new NoSufficientMetricsException(
          this,
          Duration.ofSeconds(1),
          "The log size gap of partition" + tp + "is too large, more than" + percentage);
  }

  private Optional<Double> statistReplicaSizeCount(
      TopicPartitionReplica tpr, ClusterBean clusterBean) {
    return clusterBean
        .replicaMetrics(tpr, SizeStatisticalBean.class)
        .max(Comparator.comparing(HasBeanObject::createdTimestamp))
        .map(SizeStatisticalBean::value);
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
    var brokerCost = brokerCost(clusterInfo, clusterBean).value();
    var value = dispersion.calculate(brokerCost.values());
    return () -> value;
  }

  @Override
  public PartitionCost partitionCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var result =
        clusterInfo.replicaLeaders().stream()
            .collect(
                Collectors.toMap(
                    ReplicaInfo::topicPartition,
                    leaderReplica ->
                        statistReplicaSizeCount(leaderReplica.topicPartitionReplica(), clusterBean)
                            .orElseThrow(
                                () ->
                                    new NoSufficientMetricsException(
                                        this,
                                        Duration.ofSeconds(1),
                                        "No metric for "
                                            + leaderReplica.topicPartitionReplica()))));
    return () -> result;
  }
}
