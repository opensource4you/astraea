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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.MoveCost;

public interface Balancer {

  /** @return a rebalance plan */
  Optional<Plan> offer(
      ClusterInfo<Replica> currentClusterInfo,
      Predicate<String> topicFilter,
      Map<Integer, Set<String>> brokerFolders);

  static <T extends Balancer> T create(Class<T> balancerClass, AlgorithmConfig config) {
    return Utils.packException(
        () -> balancerClass.getConstructor(AlgorithmConfig.class).newInstance(config));
  }

  class Plan {
    final RebalancePlanProposal proposal;
    final ClusterCost clusterCost;
    final List<MoveCost> moveCost;

    public RebalancePlanProposal proposal() {
      return proposal;
    }

    public ClusterCost clusterCost() {
      return clusterCost;
    }

    public List<MoveCost> moveCost() {
      return moveCost;
    }

    public Plan(RebalancePlanProposal proposal, ClusterCost clusterCost, List<MoveCost> moveCost) {
      this.proposal = proposal;
      this.clusterCost = clusterCost;
      this.moveCost = moveCost;
    }
  }
}
