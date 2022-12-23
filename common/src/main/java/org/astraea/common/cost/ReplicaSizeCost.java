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
    var result =
        clusterInfo.topicPartitionReplicas().stream()
            .collect(
                Collectors.groupingBy(
                    TopicPartitionReplica::brokerId,
                    Collectors.mapping(
                        tpr -> statistPartitionSizeCount(tpr.topicPartition(), clusterBean),
                        Collectors.summingDouble(x -> x))));
    return () -> result;
  }

  private double statistPartitionSizeCount(TopicPartition tp, ClusterBean clusterBean) {
    return clusterBean
        .partitionMetrics(tp, SizeStatisticalBean.class)
        .max(Comparator.comparing(HasBeanObject::createdTimestamp))
        .map(SizeStatisticalBean::value)
        .orElseThrow(
            () ->
                new NoSufficientMetricsException(
                    this, Duration.ofSeconds(1), "No metric for " + tp));
  }

  private double statistReplicaSizeCount(TopicPartitionReplica tpr, ClusterBean clusterBean) {
    return clusterBean
        .replicaMetrics(tpr, SizeStatisticalBean.class)
        .max(Comparator.comparing(HasBeanObject::createdTimestamp))
        .map(SizeStatisticalBean::value)
        .orElseThrow(
            () ->
                new NoSufficientMetricsException(
                    this, Duration.ofSeconds(1), "No metric for " + tpr));
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
            .map(
                leaderReplica ->
                    Map.entry(
                        leaderReplica.topicPartition(),
                        statistReplicaSizeCount(
                            leaderReplica.topicPartitionReplica(), clusterBean)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return () -> result;
  }
}
