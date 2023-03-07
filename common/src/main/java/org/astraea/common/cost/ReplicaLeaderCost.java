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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.MetricSensor;

/** more replica leaders -> higher cost */
public class ReplicaLeaderCost implements HasBrokerCost, HasClusterCost, HasMoveCost {
  private final Dispersion dispersion = Dispersion.cov();
  public static final String COST_NAME = "leader";
  private static final String COST_LIMIT_KEY = "maxMigratedLeader";
  private final Configuration moveCostLimit;

  public ReplicaLeaderCost() {
    this.moveCostLimit = Configuration.of(Map.of());
  }

  public ReplicaLeaderCost(Configuration moveCostLimit) {
    this.moveCostLimit = moveCostLimit;
  }

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        leaderCount(clusterInfo, clusterBean).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue()));
    return () -> result;
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var brokerScore = brokerCost(clusterInfo, clusterBean).value();
    var value = dispersion.calculate(brokerScore.values());
    return ClusterCost.of(
        value,
        () ->
            brokerScore.values().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", ", "{", "}")));
  }

  private static Map<Integer, Integer> leaderCount(
      ClusterInfo clusterInfo, ClusterBean clusterBean) {
    if (clusterBean == ClusterBean.EMPTY) return leaderCount(clusterInfo);
    var leaderCount = leaderCount(clusterBean);
    // if there is no available metrics, we re-count the leaders based on cluster information
    if (leaderCount.values().stream().mapToInt(i -> i).sum() == 0) return leaderCount(clusterInfo);
    return leaderCount;
  }

  static Map<Integer, Integer> leaderCount(ClusterBean clusterBean) {
    return clusterBean.all().entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    e.getValue().stream()
                        .filter(x -> x instanceof ServerMetrics.ReplicaManager.Gauge)
                        .map(x -> (ServerMetrics.ReplicaManager.Gauge) x)
                        .sorted(Comparator.comparing(HasBeanObject::createdTimestamp).reversed())
                        .limit(1)
                        .mapToInt(HasGauge::value)
                        .sum()));
  }

  static Map<Integer, Integer> leaderCount(ClusterInfo clusterInfo) {
    return clusterInfo.nodes().stream()
        .map(nodeInfo -> Map.entry(nodeInfo.id(), clusterInfo.replicaLeaders(nodeInfo.id()).size()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Optional<MetricSensor> metricSensor() {
    return Optional.of(
        (client, ignored) -> List.of(ServerMetrics.ReplicaManager.LEADER_COUNT.fetch(client)));
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var moveCost =
        Stream.concat(before.nodes().stream(), after.nodes().stream())
            .map(NodeInfo::id)
            .distinct()
            .parallel()
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    id -> {
                      var removedLeaders =
                          (int)
                              before
                                  .replicaStream(id)
                                  .filter(Replica::isLeader)
                                  .filter(
                                      r ->
                                          after
                                              .replicaStream(r.topicPartitionReplica())
                                              .noneMatch(Replica::isLeader))
                                  .count();
                      var newLeaders =
                          (int)
                              after
                                  .replicaStream(id)
                                  .filter(Replica::isLeader)
                                  .filter(
                                      r ->
                                          before
                                              .replicaStream(r.topicPartitionReplica())
                                              .noneMatch(Replica::isLeader))
                                  .count();
                      return newLeaders - removedLeaders;
                    }));
    var maxMigratedLeader =
        moveCostLimit.string(COST_LIMIT_KEY).map(Long::parseLong).orElse(Long.MAX_VALUE);
    var overflow =
        maxMigratedLeader < moveCost.values().stream().map(Math::abs).mapToLong(s -> s).sum();
    return MoveCost.changedReplicaLeaderCount(moveCost, overflow);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
