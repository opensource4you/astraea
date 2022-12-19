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
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.collector.Fetcher;

/** more replica leaders -> higher cost */
public class ReplicaLeaderCost implements HasBrokerCost, HasClusterCost, HasMoveCost {
  private final Dispersion dispersion = Dispersion.cov();
  public static final String COST_NAME = "leader";

  @Override
  public BrokerCost brokerCost(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
    var result =
        leaderCount(clusterInfo, clusterBean).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue()));
    return () -> result;
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
    var brokerScore = brokerCost(clusterInfo, clusterBean).value();
    var value = dispersion.calculate(brokerScore.values());
    return () -> value;
  }

  private static Map<Integer, Integer> leaderCount(
      ClusterInfo<? extends ReplicaInfo> clusterInfo, ClusterBean clusterBean) {
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

  static Map<Integer, Integer> leaderCount(ClusterInfo<? extends ReplicaInfo> clusterInfo) {
    return clusterInfo.nodes().stream()
        .map(nodeInfo -> Map.entry(nodeInfo.id(), clusterInfo.replicaLeaders(nodeInfo.id()).size()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(c -> List.of(ServerMetrics.ReplicaManager.LEADER_COUNT.fetch(c)));
  }

  @Override
  public MoveCost moveCost(
      ClusterInfo<Replica> before, ClusterInfo<Replica> after, ClusterBean clusterBean) {
    return MoveCost.changedReplicaLeaderCount(
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
                                  .filter(ReplicaInfo::isLeader)
                                  .filter(
                                      r ->
                                          after
                                              .replicaStream(r.topicPartitionReplica())
                                              .noneMatch(ReplicaInfo::isLeader))
                                  .count();
                      var newLeaders =
                          (int)
                              after
                                  .replicaStream(id)
                                  .filter(ReplicaInfo::isLeader)
                                  .filter(
                                      r ->
                                          before
                                              .replicaStream(r.topicPartitionReplica())
                                              .noneMatch(ReplicaInfo::isLeader))
                                  .count();
                      return newLeaders - removedLeaders;
                    })));
  }
}
