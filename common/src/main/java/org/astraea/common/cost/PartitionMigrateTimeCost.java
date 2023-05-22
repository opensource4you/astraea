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

import static org.astraea.common.cost.MigrationCost.brokerMaxRate;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.Sensor;
import org.astraea.common.metrics.broker.HasMaxRate;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.MetricSensor;
import org.astraea.common.metrics.stats.Max;

/** MoveCost: more max write rate change -> higher migrate cost. */
public class PartitionMigrateTimeCost implements HasMoveCost {
  private static final String REPLICATION_IN_RATE = "replication_in_rate";
  private static final String REPLICATION_OUT_RATE = "replication_out_rate";
  static final String MAX_MIGRATE_TIME_KEY = "max.migrated.time.limit";
  public static final String STATISTICS_RATE_KEY = "statistics.rate.key";

  // metrics windows size
  private final Duration maxMigrateTime;

  public PartitionMigrateTimeCost(Configuration config) {
    this.maxMigrateTime =
        config.duration(MAX_MIGRATE_TIME_KEY).orElse(Duration.ofSeconds(Long.MAX_VALUE));
  }

  @Override
  public MetricSensor metricSensor() {
    return (client, clusterBean) -> {
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
      var maxRateSensor =
          Sensor.builder()
              .addStats(
                  Map.of(
                      REPLICATION_IN_RATE, Max.<Double>of(),
                      REPLICATION_OUT_RATE, Max.of()))
              .build();
      maxRateSensor.record(REPLICATION_IN_RATE, newInMetrics.oneMinuteRate());
      maxRateSensor.record(REPLICATION_OUT_RATE, newOutMetrics.oneMinuteRate());
      var inRate = maxRateSensor.measure(REPLICATION_IN_RATE);
      var outRate = maxRateSensor.measure(REPLICATION_OUT_RATE);
      return List.of(
          new MaxReplicationInRateBean(
              () ->
                  new BeanObject(
                      newInMetrics.beanObject().domainName(),
                      newInMetrics.beanObject().properties(),
                      Map.of(STATISTICS_RATE_KEY, Math.max(oldInRate.orElse(0), inRate)),
                      current.toMillis())),
          new MaxReplicationOutRateBean(
              () ->
                  new BeanObject(
                      newOutMetrics.beanObject().domainName(),
                      newOutMetrics.beanObject().properties(),
                      Map.of(STATISTICS_RATE_KEY, Math.max(oldOutRate.orElse(0), outRate)),
                      current.toMillis())));
    };
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var brokerMigrateSecond = MigrationCost.brokerMigrationSecond(before, after, clusterBean);
    var planMigrateSecond =
        brokerMigrateSecond.values().stream()
            .max(Comparator.comparing(Function.identity()))
            .orElse(Long.MAX_VALUE);
    return () -> planMigrateSecond > this.maxMigrateTime.getSeconds();
  }

  public record MaxReplicationInRateBean(HasMaxRate hasMaxRate) implements HasMaxRate {
    @Override
    public BeanObject beanObject() {
      return hasMaxRate.beanObject();
    }
  }

  public record MaxReplicationOutRateBean(HasMaxRate hasMaxRate) implements HasMaxRate {
    @Override
    public BeanObject beanObject() {
      return hasMaxRate.beanObject();
    }
  }
}
