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
package org.astraea.common.balancer;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.MoveCost;

public interface Balancer {

  /** @return a rebalance plan */
  default Optional<Plan> offer(
      ClusterInfo<Replica> clusterInfo, Map<Integer, Set<String>> brokerFolders) {
    return offer(clusterInfo, ignore -> true, brokerFolders);
  }

  /** @return a rebalance plan */
  Optional<Plan> offer(
      ClusterInfo<Replica> clusterInfo,
      Predicate<String> topicFilter,
      Map<Integer, Set<String>> brokerFolders);

  static BalancerBuilder builder() {
    return new BalancerBuilder();
  }

  class Plan {
    final RebalancePlanProposal proposal;
    final ClusterCost clusterCost;
    final MoveCost moveCost;

    public RebalancePlanProposal proposal() {
      return proposal;
    }

    public ClusterCost clusterCost() {
      return clusterCost;
    }

    public MoveCost moveCost() {
      return moveCost;
    }

    public Plan(RebalancePlanProposal proposal, ClusterCost clusterCost, MoveCost moveCost) {
      this.proposal = proposal;
      this.clusterCost = clusterCost;
      this.moveCost = moveCost;
    }
  }
}
