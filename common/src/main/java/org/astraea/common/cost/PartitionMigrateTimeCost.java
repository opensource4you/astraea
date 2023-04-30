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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.ClusterBean;
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
          var oldInRate =
              brokerMaxRate(
                  client.identity(),
                  clusterBean,
                  PartitionMigrateTimeCost.MaxReplicationInRateBean.class);
          var oldOutRate =
              brokerMaxRate(
                  client.identity(),
                  clusterBean,
                  PartitionMigrateTimeCost.MaxReplicationOutRateBean.class);
          var newInMetrics = ServerMetrics.BrokerTopic.REPLICATION_BYTES_IN_PER_SEC.fetch(client);
          var newOutMetrics = ServerMetrics.BrokerTopic.REPLICATION_BYTES_OUT_PER_SEC.fetch(client);
          var current = Duration.ofMillis(System.currentTimeMillis());
          var maxInRateSensor =
              Sensor.builder().addStat(REPLICATION_IN_RATE, Max.<Double>of()).build();
          var maxOutRateSensor =
              Sensor.builder().addStat(REPLICATION_OUT_RATE, Max.<Double>of()).build();
          maxInRateSensor.record(newInMetrics.oneMinuteRate());
          maxOutRateSensor.record(newOutMetrics.oneMinuteRate());
          var inRate = maxInRateSensor.measure(REPLICATION_IN_RATE);
          var outRate = maxOutRateSensor.measure(REPLICATION_OUT_RATE);

          var metrics = new ArrayList<HasBeanObject>();
          metrics.add(
              (MaxReplicationInRateBean)
                  () ->
                      new BeanObject(
                          newInMetrics.beanObject().domainName(),
                          newInMetrics.beanObject().properties(),
                          Map.of(HasRate.ONE_MIN_RATE_KEY, Math.max(oldInRate.orElse(0), inRate)),
                          current.toMillis()));
          metrics.add(
              (MaxReplicationOutRateBean)
                  () ->
                      new BeanObject(
                          newOutMetrics.beanObject().domainName(),
                          newOutMetrics.beanObject().properties(),
                          Map.of(HasRate.ONE_MIN_RATE_KEY, Math.max(oldOutRate.orElse(0), outRate)),
                          current.toMillis()));
          return metrics;
        });
  }

  public static OptionalDouble brokerMaxRate(
      int identity, ClusterBean clusterBean, Class<? extends HasBeanObject> statisticMetrics) {
    return clusterBean.all().getOrDefault(identity, List.of()).stream()
        .filter(x -> statisticMetrics.isAssignableFrom(x.getClass()))
        .mapToDouble(x -> ((HasMeter) x).oneMinuteRate())
        .max();
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var brokerMigrateTime = MigrationCost.brokerMigrationTime(before, after, clusterBean);
    var maxMigrateTime =
        Stream.concat(before.nodes().stream(), after.nodes().stream())
            .distinct()
            .map(nodeInfo -> brokerMigrateTime.get(nodeInfo.id()))
            .max(Comparator.comparing(Function.identity()))
            .orElse(Long.MAX_VALUE);
    return () -> maxMigrateTime > this.maxMigrateTime;
  }

  public interface MaxReplicationInRateBean extends HasMeter {}

  public interface MaxReplicationOutRateBean extends HasMeter {}
}
