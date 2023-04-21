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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.Sensor;
import org.astraea.common.metrics.broker.HasMeter;
import org.astraea.common.metrics.broker.HasRate;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.MetricSensor;
import org.astraea.common.metrics.stats.Max;

/** MoveCost: more max write rate change -> higher migrate cost. */
public class PartitionMigrateTimeCost implements HasMoveCost {
  private static final String REPLICATION_IN_RATE = "replication_in_rate";
  private static final String REPLICATION_OUT_RATE = "replication_out_rate";
  public static final String MAX_MIGRATE_TIME_KEY = "max.migrated.time.limit";
  static final Map<Integer, Sensor<Double>> maxBrokerReplicationInRate = new HashMap<>();
  static final Map<Integer, Sensor<Double>> maxBrokerReplicationOutRate = new HashMap<>();
  // metrics windows size
  private final long maxMigrateTime;

  public PartitionMigrateTimeCost(Configuration config) {
    this.maxMigrateTime =
        config.string(MAX_MIGRATE_TIME_KEY).map(Long::parseLong).orElse(Long.MAX_VALUE);
  }

  @Override
  public Optional<MetricSensor> metricSensor() {
    return Optional.of(
        (client, clusterBean) -> {
          var metrics =
              clusterBean.all().values().stream()
                  .flatMap(Collection::stream)
                  .distinct()
                  .collect(Collectors.toCollection(ArrayList::new));
          var newInMetrics = ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC.fetch(client);
          var newOutMetrics = ServerMetrics.BrokerTopic.REPLICATION_BYTES_OUT_PER_SEC.fetch(client);
          var current = Duration.ofMillis(System.currentTimeMillis());
          var maxInRateSensor =
              maxBrokerReplicationInRate.computeIfAbsent(
                  client.identity(),
                  ignore ->
                      Sensor.builder().addStat(REPLICATION_IN_RATE, Max.<Double>of()).build());
          var maxOutRateSensor =
              maxBrokerReplicationOutRate.computeIfAbsent(
                  client.identity(),
                  ignore ->
                      Sensor.builder().addStat(REPLICATION_OUT_RATE, Max.<Double>of()).build());
          maxInRateSensor.record(newInMetrics.oneMinuteRate());
          maxOutRateSensor.record(newOutMetrics.oneMinuteRate());
          var inRate = maxInRateSensor.measure(REPLICATION_IN_RATE);
          var outRate = maxOutRateSensor.measure(REPLICATION_OUT_RATE);
          metrics.add(
              (MaxReplicationInRateBean)
                  () ->
                      new BeanObject(
                          newInMetrics.beanObject().domainName(),
                          newInMetrics.beanObject().properties(),
                          Map.of(HasRate.ONE_MIN_RATE_KEY, inRate),
                          current.toMillis()));
          metrics.add(
              (MaxReplicationOutRateBean)
                  () ->
                      new BeanObject(
                          newOutMetrics.beanObject().domainName(),
                          newOutMetrics.beanObject().properties(),
                          Map.of(HasRate.ONE_MIN_RATE_KEY, outRate),
                          current.toMillis()));
          return metrics;
        });
  }

  public static Map<Integer, OptionalDouble> brokerMaxRate(
      ClusterInfo clusterInfo,
      ClusterBean clusterBean,
      Class<? extends HasBeanObject> statisticMetrics) {
    return clusterInfo.brokers().stream()
        .map(
            broker ->
                Map.entry(
                    broker.id(),
                    clusterBean.all().getOrDefault(broker.id(), List.of()).stream()
                        .filter(x -> statisticMetrics.isAssignableFrom(x.getClass()))
                        .mapToDouble(x -> ((HasMeter) x).oneMinuteRate())
                        .max()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public Map<Integer, Double> brokerMigratedTime(
      Map<Integer, Long> needToMigrated, Map<Integer, OptionalDouble> brokerRate) {
    return needToMigrated.entrySet().stream()
        .map(
            brokerSize ->
                Map.entry(
                    brokerSize.getKey(),
                    brokerSize.getValue()
                        / brokerRate
                            .get(brokerSize.getKey())
                            .orElseThrow(
                                () ->
                                    new NoSufficientMetricsException(
                                        this,
                                        Duration.ofSeconds(1),
                                        "No metric for broker" + brokerSize.getKey()))))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var brokerInRate = brokerMaxRate(before, clusterBean, MaxReplicationInRateBean.class);
    var brokerOutRate = brokerMaxRate(before, clusterBean, MaxReplicationOutRateBean.class);
    var needToMigrateIn = MigrationCost.recordSizeToFetch(before, after);
    var needToMigrateOut = MigrationCost.recordSizeToSync(before, after);

    var brokerMigrateInTime = brokerMigratedTime(needToMigrateIn, brokerInRate);
    var brokerMigrateOutTime = brokerMigratedTime(needToMigrateOut, brokerOutRate);
    var maxMigrateTime =
        Stream.concat(before.nodes().stream(), after.nodes().stream())
            .distinct()
            .map(
                nodeInfo ->
                    Math.max(
                        brokerMigrateInTime.get(nodeInfo.id()),
                        brokerMigrateOutTime.get(nodeInfo.id())))
            .max(Comparator.comparing(Function.identity()))
            .orElse(0.0);
    return () -> maxMigrateTime > this.maxMigrateTime;
  }

  public interface MaxReplicationInRateBean extends HasMeter {}

  public interface MaxReplicationOutRateBean extends HasMeter {}
}
