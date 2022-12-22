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

import static org.astraea.common.metrics.stats.Avg.expWeightByTime;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
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

public class ReplicaDiskInCost implements HasClusterCost, HasBrokerCost {
  private static final Dispersion dispersion = Dispersion.cov();
  private static final String REPLICA_WRITE_RATE = "replica_write_rate";
  static final Map<TopicPartitionReplica, Double> lastRecord = new HashMap<>();
  static final Map<TopicPartitionReplica, Duration> lastTime = new HashMap<>();
  static final Map<TopicPartitionReplica, Sensor<Double>> expWeightSensors = new HashMap<>();
  static final Map<TopicPartitionReplica, Debounce<Double>> denounces = new HashMap<>();

  @Override
  public ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
    var brokerCost = brokerCost(clusterInfo, clusterBean).value();
    var value = dispersion.calculate(brokerCost.values());
    return () -> value;
  }

  @Override
  public BrokerCost brokerCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var partitionCost = partitionCost(clusterInfo, clusterBean);
    var brokerLoad =
        clusterInfo.nodes().stream()
            .map(
                node ->
                    Map.entry(
                        node.id(),
                        partitionCost.apply(node.id()).values().stream()
                            .mapToDouble(rate -> rate)
                            .sum()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return () -> brokerLoad;
  }

  private Function<Integer, Map<TopicPartition, Double>> partitionCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var replicaIn =
        clusterInfo.topicPartitionReplicas().stream()
            .collect(
                Collectors.toMap(
                    tpr -> tpr,
                    tpr ->
                            statistPartitionSizeCount(tpr.topicPartition(), clusterBean)));
    var scoreForBroker =
        clusterInfo.nodes().stream()
            .map(
                node ->
                    Map.entry(
                        node.id(),
                        replicaIn.entrySet().stream()
                            .filter(x -> x.getKey().brokerId() == node.id())
                            .collect(
                                Collectors.groupingBy(
                                    x ->
                                        TopicPartition.of(
                                            x.getKey().topic(), x.getKey().partition())))
                            .entrySet()
                            .stream()
                            .map(
                                entry ->
                                    Map.entry(
                                        entry.getKey(),
                                        entry.getValue().stream()
                                            .mapToDouble(Map.Entry::getValue)
                                            .max()
                                            .orElseThrow()))
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    Map.Entry::getKey, Map.Entry::getValue))))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return scoreForBroker::get;
  }

  private double statistPartitionSizeCount(TopicPartition tp, ClusterBean clusterBean) {
    return clusterBean
        .partitionMetrics(tp, LogRateStatisticalBean.class)
        .max(Comparator.comparing(HasBeanObject::createdTimestamp))
        .map(LogRateStatisticalBean::value)
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
                                          .addStat(
                                              REPLICA_WRITE_RATE,
                                              expWeightByTime(Duration.ofSeconds(1)))
                                          .build());
                          var debounce =
                              denounces.computeIfAbsent(
                                  tpr, ignore -> Debounce.of(Duration.ofSeconds(1)));
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
                          return (ReplicaDiskInCost.LogRateStatisticalBean)
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

  public interface LogRateStatisticalBean extends HasGauge<Double> {}
}
