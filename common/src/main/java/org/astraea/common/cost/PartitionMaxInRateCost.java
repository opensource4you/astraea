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
import org.astraea.common.DataRate;
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
import org.astraea.common.metrics.stats.Debounce;
import org.astraea.common.metrics.stats.Max;

/** MoveCost: more max write rate change -> higher migrate cost. */
public class PartitionMaxInRateCost implements HasMoveCost {
  private static final String REPLICA_WRITE_RATE = "replica_write_rate";
  private static final Duration DEFAULT_DURATION = Duration.ofSeconds(1);
  private final Duration duration;
  static final Map<TopicPartitionReplica, Double> lastRecord = new HashMap<>();
  static final Map<TopicPartitionReplica, Duration> lastTime = new HashMap<>();
  static final Map<TopicPartitionReplica, Sensor<Double>> expWeightSensors = new HashMap<>();
  static final Map<TopicPartitionReplica, Debounce<Double>> denounces = new HashMap<>();

  PartitionMaxInRateCost() {
    this.duration = DEFAULT_DURATION;
  }

  PartitionMaxInRateCost(Duration duration) {
    this.duration = duration;
  }

  public Map<TopicPartition, Double> partitionCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var replicaRate =
        clusterBean.replicas().stream()
            .map(
                p ->
                    clusterBean
                        .replicaMetrics(p, WorseLogRateStatisticalBean.class)
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
    var partitionIn =
        clusterInfo.topicPartitions().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    topicPartition ->
                        statistPartitionRateCount(
                                clusterInfo
                                    .replicaLeader(topicPartition)
                                    .map(ReplicaInfo::topicPartitionReplica)
                                    .get(),
                                clusterBean)
                            .orElse(
                                maxPartitionRate(
                                    topicPartition,
                                    replicaRate.getOrDefault(topicPartition, List.of())))));

    if (partitionIn.values().stream().allMatch(x -> x == 0.0))
      throw new NoSufficientMetricsException(
          this, Duration.ofSeconds(1), "all topic partitions are currently idle.");
    return partitionIn;
  }

  private Optional<Double> statistPartitionRateCount(
      TopicPartitionReplica tpr, ClusterBean clusterBean) {
    return clusterBean
        .replicaMetrics(tpr, WorseLogRateStatisticalBean.class)
        .max(Comparator.comparing(HasBeanObject::createdTimestamp))
        .map(WorseLogRateStatisticalBean::value);
  }

  double maxPartitionRate(TopicPartition tp, List<Double> size) {
    if (size.isEmpty())
      throw new NoSufficientMetricsException(this, Duration.ofSeconds(1), "No metric for " + tp);
    return size.stream()
        .max(Comparator.comparing(Function.identity()))
        .orElseThrow(
            () ->
                new NoSufficientMetricsException(
                    this, Duration.ofSeconds(1), "No metric for " + tp));
  }

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
                          var current = Duration.ofMillis(System.currentTimeMillis());
                          var tpr = TopicPartitionReplica.of(g.topic(), g.partition(), identity);
                          var size = Double.valueOf(g.value());
                          var replicaRateSensor =
                              expWeightSensors.computeIfAbsent(
                                  tpr,
                                  ignore ->
                                      Sensor.builder()
                                          .addStat(REPLICA_WRITE_RATE, new Max<Double>())
                                          .build());
                          var debounce =
                              denounces.computeIfAbsent(tpr, ignore -> Debounce.of(duration));
                          debounce
                              .record(size, current.toMillis())
                              .ifPresent(
                                  debouncedValue -> {
                                    if (lastTime.containsKey(tpr))
                                      replicaRateSensor.record(
                                          (debouncedValue
                                              - lastRecord.getOrDefault(tpr, 0.0)
                                                  / (current.getSeconds()
                                                      - lastTime.get(tpr).getSeconds())));
                                    lastTime.put(tpr, current);
                                    lastRecord.put(tpr, debouncedValue);
                                  });
                          return (WorseLogRateStatisticalBean)
                              () ->
                                  new BeanObject(
                                      g.beanObject().domainName(),
                                      g.beanObject().properties(),
                                      Map.of(
                                          HasGauge.VALUE_KEY,
                                          replicaRateSensor.measure(REPLICA_WRITE_RATE)),
                                      System.currentTimeMillis());
                        })
                    .collect(Collectors.toList())));
  }

  @Override
  public MoveCost moveCost(
      ClusterInfo<Replica> before, ClusterInfo<Replica> after, ClusterBean clusterBean) {
    var partitionIn = partitionCost(before, clusterBean);
    return MoveCost.changedReplicaMaxInRate(
        Stream.concat(before.nodes().stream(), after.nodes().stream())
            .map(NodeInfo::id)
            .distinct()
            .parallel()
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    id ->
                        DataRate.Byte.of(
                                Math.round(
                                    after
                                            .replicaStream(id)
                                            .mapToDouble(r -> partitionIn.get(r.topicPartition()))
                                            .sum()
                                        - before
                                            .replicaStream(id)
                                            .mapToDouble(r -> partitionIn.get(r.topicPartition()))
                                            .sum()))
                            .perSecond())));
  }

  public interface WorseLogRateStatisticalBean extends HasGauge<Double> {}
}
