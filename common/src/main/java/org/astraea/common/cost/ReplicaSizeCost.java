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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
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
import org.astraea.common.metrics.SensorBuilder;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.collector.Fetcher;
import org.astraea.common.metrics.collector.MetricSensor;
import org.astraea.common.metrics.stats.Avg;

public class ReplicaSizeCost
    implements HasMoveCost, HasBrokerCost, HasClusterCost, HasPartitionCost {
  private final Dispersion dispersion = Dispersion.correlationCoefficient();
  public static final String COST_NAME = "size";

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
        new MetricSensor() {
          final Map<TopicPartitionReplica, Sensor<Double>> sensors = new HashMap<>();

          @Override
          public Map<Integer, Collection<? extends HasBeanObject>> record(
              int identity, Collection<? extends HasBeanObject> beans) {
            var statisticalBeans = new HashMap<TopicPartitionReplica, HasBeanObject>();
            beans.forEach(
                bean -> {
                  if (bean != null) {
                    if (bean.beanObject().domainName().equals(LogMetrics.DOMAIN_NAME)
                        && bean.beanObject().properties().get("type").equals(LogMetrics.LOG_TYPE)) {
                      var tpr =
                          TopicPartitionReplica.of(
                              bean.beanObject().properties().get("topic"),
                              Integer.parseInt(bean.beanObject().properties().get("partition")),
                              identity);
                      sensors
                          .computeIfAbsent(
                              tpr,
                              ignore ->
                                  new SensorBuilder<Double>()
                                      .addStat(
                                          Avg.EXP_WEIGHT_BY_TIME_KEY,
                                          Avg.expWeightByTime(Duration.ofSeconds(1)))
                                      .build())
                          .record(
                              Double.valueOf(
                                  bean.beanObject().attributes().get("Value").toString()));
                      statisticalBeans.put(tpr, bean);
                    }
                  }
                });
            return Map.of(
                identity,
                sensors.entrySet().stream()
                    .map(
                        sensor -> {
                          var bean = statisticalBeans.get(sensor.getKey()).beanObject();
                          return new SizeStatisticalBean(
                              new BeanObject(
                                  bean.domainName(),
                                  bean.properties(),
                                  Map.of(
                                      "Value",
                                      sensor.getValue().measure(Avg.EXP_WEIGHT_BY_TIME_KEY)),
                                  bean.createdTimestamp()));
                        })
                    .collect(Collectors.toList()));
          }
        });
  }

  public static class SizeStatisticalBean implements HasGauge<Long> {
    BeanObject beanObject;

    SizeStatisticalBean(BeanObject beanObject) {
      this.beanObject = beanObject;
    }

    @Override
    public BeanObject beanObject() {
      return beanObject;
    }
  }

  @Override
  public MoveCost moveCost(
      ClusterInfo<Replica> before, ClusterInfo<Replica> after, ClusterBean clusterBean) {
    var removedReplicas = ClusterInfo.diff(before, after);
    var addedReplicas = ClusterInfo.diff(after, before);
    var migrateInfo = migrateInfo(removedReplicas, addedReplicas);
    var sizeChanges = migrateInfo.sizeChange;
    var totalMigrateSize = migrateInfo.totalMigrateSize;
    return new MoveCost() {
      @Override
      public String name() {
        return COST_NAME;
      }

      @Override
      public long totalCost() {
        return totalMigrateSize;
      }

      @Override
      public String unit() {
        return "byte";
      }

      @Override
      public Map<Integer, Long> changes() {
        return sizeChanges;
      }
    };
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a BrokerCost contains the used space for each broker
   */
  @Override
  public BrokerCost brokerCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var result = sizeCount(clusterInfo, clusterBean);
    return () -> result;
  }

  private Map<Integer, Double> sizeCount(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var statistBeans =
        clusterInfo.topicPartitionReplicas().stream()
            .map(
                tpr ->
                    Map.entry(
                        tpr,
                        clusterBean
                            .replicaMetrics(tpr, SizeStatisticalBean.class)
                            .max(Comparator.comparing(HasBeanObject::createdTimestamp))
                            .map(SizeStatisticalBean::value)
                            .orElse(0L)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    // if there is no statistBeans, use ClusterBean to calculate brokerCost
    if (statistBeans.isEmpty()) return sizeCount(clusterBean);
    return clusterInfo.nodes().stream()
        .collect(
            Collectors.toMap(
                NodeInfo::id,
                nodeInfo ->
                    statistBeans.entrySet().stream()
                        .filter(e -> e.getKey().brokerId() == nodeInfo.id())
                        .mapToDouble(Map.Entry::getValue)
                        .sum()));
  }

  private Map<Integer, Double> sizeCount(ClusterBean clusterBean) {
    return clusterBean.all().entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    LogMetrics.Log.gauges(e.getValue(), LogMetrics.Log.SIZE).stream()
                        .mapToDouble(LogMetrics.Log.Gauge::value)
                        .sum()));
  }

  private Map<Integer, Double> sizeCount(ClusterInfo<? extends Replica> clusterInfo) {
    return clusterInfo.nodes().stream()
        .collect(
            Collectors.toMap(
                NodeInfo::id,
                nodeInfo ->
                    Double.longBitsToDouble(
                        clusterInfo.replicas().stream()
                            .filter(r -> r.nodeInfo().id() == nodeInfo.id())
                            .mapToLong(Replica::size)
                            .sum())));
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
    var brokerCost = brokerCost(clusterInfo, clusterBean).value();
    if (brokerCost.isEmpty()) brokerCost = sizeCount(clusterInfo);
    var value = dispersion.calculate(brokerCost.values());
    return () -> value;
  }

  @Override
  public PartitionCost partitionCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    return () ->
        clusterBean.replicas().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    tpr -> TopicPartition.of(tpr.topic(), tpr.partition()),
                    tpr ->
                        clusterBean
                            .replicaMetrics(tpr, LogMetrics.Log.Gauge.class)
                            .filter(gauge -> gauge.type().equals(LogMetrics.Log.SIZE))
                            .mapToDouble(HasGauge::value)
                            .sum()));
  }

  static class MigrateInfo {
    long totalMigrateSize;
    Map<Integer, Long> sizeChange;

    MigrateInfo(long totalMigrateSize, Map<Integer, Long> sizeChange) {
      this.totalMigrateSize = totalMigrateSize;
      this.sizeChange = sizeChange;
    }
  }

  static MigrateInfo migrateInfo(
      Collection<Replica> removedReplicas, Collection<Replica> addedReplicas) {
    var changes = new HashMap<Integer, Long>();
    AtomicLong totalMigrateSize = new AtomicLong(0L);
    removedReplicas.forEach(
        replica ->
            changes.compute(
                replica.nodeInfo().id(),
                (ignore, size) -> size == null ? -replica.size() : -replica.size() + size));

    addedReplicas.forEach(
        replica ->
            changes.compute(
                replica.nodeInfo().id(),
                (ignore, size) -> {
                  totalMigrateSize.set(totalMigrateSize.get() + replica.size());
                  return size == null ? replica.size() : replica.size() + size;
                }));
    return new MigrateInfo(totalMigrateSize.get(), changes);
  }
}
