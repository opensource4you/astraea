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

import static org.astraea.common.cost.CostUtils.changedRecordSizeOverflow;

import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.ClusterBean;

/**
 * PartitionCost: more replica log size -> higher partition score BrokerCost: more replica log size
 * in broker -> higher broker cost ClusterCost: The more unbalanced the replica log size among
 * brokers -> higher cluster cost MoveCost: more replicas log size migrate
 */
public class ReplicaLeaderSizeCost
    implements HasMoveCost, HasBrokerCost, HasClusterCost, HasPartitionCost {
  private final Configuration config;

  public static final String COST_LIMIT_KEY = "max.migrated.leader.size";
  public static final String MOVED_LEADER_SIZE = "moved leader size (bytes)";

  public ReplicaLeaderSizeCost() {
    this.config = new Configuration(Map.of());
  }

  public ReplicaLeaderSizeCost(Configuration config) {
    this.config = config;
  }

  private final Dispersion dispersion = Dispersion.standardDeviation();

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var maxMigratedLeaderSize =
        config.string(COST_LIMIT_KEY).map(DataSize::of).map(DataSize::bytes).orElse(Long.MAX_VALUE);
    var overflow =
        changedRecordSizeOverflow(before, after, Replica::isLeader, maxMigratedLeaderSize);
    return () -> overflow;
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a BrokerCost contains the used space for each broker
   */
  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        clusterInfo.replicas().stream()
            .collect(
                Collectors.groupingBy(
                    r -> r.brokerId(),
                    Collectors.mapping(
                        r ->
                            clusterInfo
                                .replicaLeader(r.topicPartition())
                                .map(Replica::size)
                                .orElseThrow(),
                        Collectors.summingDouble(x -> x))));
    return () -> result;
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var brokerCost = brokerCost(clusterInfo, clusterBean).value();
    var value = dispersion.calculate(brokerCost.values());
    return ClusterCost.of(
        value,
        () ->
            brokerCost.values().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", ", "{", "}")));
  }

  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        clusterInfo.replicaLeaders().stream()
            .collect(Collectors.toMap(Replica::topicPartition, r -> (double) r.size()));
    return () -> result;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
