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

import static org.astraea.common.cost.MigrationCost.replicaLeaderToAdd;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.MetricSensor;

/** more replica leaders -> higher cost */
public class ReplicaLeaderCost implements HasBrokerCost, HasClusterCost, HasMoveCost {
  private final Dispersion dispersion = Dispersion.cov();
  private final Configuration config;
  public static final String MAX_MIGRATE_LEADER_KEY = "max.migrated.leader.number";

  public ReplicaLeaderCost() {
    this.config = Configuration.of(Map.of());
  }

  public ReplicaLeaderCost(Configuration config) {
    this.config = config;
  }

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        leaderCount(clusterInfo).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue()));
    return () -> result;
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var brokerScore = leaderCount(clusterInfo);
    var value = dispersion.calculate(brokerScore.values());
    return ClusterCost.of(
        value,
        () ->
            brokerScore.values().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", ", "{", "}")));
  }

  static Map<Integer, Integer> leaderCount(ClusterInfo clusterInfo) {
    return clusterInfo.nodes().stream()
        .map(nodeInfo -> Map.entry(nodeInfo.id(), clusterInfo.replicaLeaders(nodeInfo.id()).size()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public MetricSensor metricSensor() {
    return (client, ignored) -> List.of(ServerMetrics.ReplicaManager.LEADER_COUNT.fetch(client));
  }

  public Configuration config() {
    return this.config;
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var replicaLeaderIn = replicaLeaderToAdd(before, after);
    var maxMigratedLeader =
        config.string(MAX_MIGRATE_LEADER_KEY).map(Long::parseLong).orElse(Long.MAX_VALUE);
    var overflow =
        maxMigratedLeader
            < replicaLeaderIn.values().stream().map(Math::abs).mapToLong(s -> s).sum();
    return () -> overflow;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
