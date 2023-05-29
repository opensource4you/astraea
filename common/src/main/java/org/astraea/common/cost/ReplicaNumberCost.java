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

import static org.astraea.common.cost.MigrationCost.replicaNumChanged;

import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.ClusterBean;

/** more replicas migrate -> higher cost */
public class ReplicaNumberCost implements HasClusterCost, HasMoveCost {
  public static final String COST_LIMIT_KEY = "max.migrated.replica.number";

  private final Configuration config;

  public ReplicaNumberCost() {
    this.config = new Configuration(Map.of());
  }

  public ReplicaNumberCost(Configuration config) {
    this.config = config;
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var moveCost = replicaNumChanged(before, after);
    var maxMigratedReplicas =
        config.string(COST_LIMIT_KEY).map(Long::parseLong).orElse(Long.MAX_VALUE);
    var overflow =
        maxMigratedReplicas < moveCost.values().stream().map(Math::abs).mapToLong(s -> s).sum();
    return () -> overflow;
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var totalReplicas = clusterInfo.replicas().size();

    // no need to rebalance
    if (totalReplicas == 0) return ClusterCost.of(0, () -> "no replica");

    var replicaPerBroker =
        clusterInfo
            .replicaStream()
            .collect(Collectors.groupingBy(r -> r.brokerId(), Collectors.counting()));
    var summary = replicaPerBroker.values().stream().mapToLong(x -> x).summaryStatistics();

    var anyBrokerEmpty =
        clusterInfo.brokers().stream()
            .map(Broker::id)
            .anyMatch(alive -> !replicaPerBroker.containsKey(alive));
    var max = summary.getMax();
    var min = anyBrokerEmpty ? 0 : summary.getMin();
    // complete balance
    if (max - min == 0) return ClusterCost.of(0, () -> "complete balance " + max);
    // complete balance in terms of integer
    // The following case will trigger if the number of replicas is not integer times of brokers.
    // For example: allocate 4 replicas to 3 brokers. The ideal placement state will be (2,1,1),
    // (1,2,1) or (1,1,2). All these cases should be considered as optimal solution since the number
    // of replica must be integer. And this case will be trigger if the (max - min) equals 1. If
    // such case is detected, return 0 as the optimal state of this cost function was found.
    if (max - min == 1) return ClusterCost.of(0, () -> "integer balance " + max);
    return ClusterCost.of((double) (max - min) / (totalReplicas), summary::toString);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
