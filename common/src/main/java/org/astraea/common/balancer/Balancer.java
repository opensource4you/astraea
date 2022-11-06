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
import org.astraea.common.EnumInfo;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.balancer.reports.BalancerProgressReport;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.MoveCost;

public interface Balancer {

  /**
   * @return a rebalance plan
   */
  default Optional<Plan> offer(
      ClusterInfo<Replica> currentClusterInfo, Map<Integer, Set<String>> brokerFolders) {
    return offer(currentClusterInfo, brokerFolders, BalancerProgressReport.EMPTY);
  }

  /**
   * @return a rebalance plan
   */
  Optional<Plan> offer(
      ClusterInfo<Replica> currentClusterInfo,
      Map<Integer, Set<String>> brokerFolders,
      BalancerProgressReport progressReport);

  /**
   * Initialize an instance of specific Balancer implementation
   *
   * @param balancerClass the class of the balancer implementation
   * @param config the algorithm configuration for the new instance
   * @return a {@link Balancer} instance of the given class
   */
  static <T extends Balancer> T create(Class<T> balancerClass, AlgorithmConfig config) {
    try {
      // case 0: create the class by the given configuration
      var constructor = balancerClass.getConstructor(AlgorithmConfig.class);
      return Utils.packException(() -> constructor.newInstance(config));
    } catch (NoSuchMethodException e) {
      // case 1: create the class by empty constructor
      return Utils.packException(() -> balancerClass.getConstructor().newInstance());
    }
  }

  class Plan {
    final ClusterLogAllocation proposal;
    final ClusterCost clusterCost;
    final List<MoveCost> moveCost;

    public ClusterLogAllocation proposal() {
      return proposal;
    }

    public ClusterCost clusterCost() {
      return clusterCost;
    }

    public List<MoveCost> moveCost() {
      return moveCost;
    }

    public Plan(ClusterLogAllocation proposal, ClusterCost clusterCost, List<MoveCost> moveCost) {
      this.proposal = proposal;
      this.clusterCost = clusterCost;
      this.moveCost = moveCost;
    }
  }

  /** The official implementation of {@link Balancer}. */
  enum Official implements EnumInfo {
    SingleStep(SingleStepBalancer.class),
    Greedy(GreedyBalancer.class);

    private final Class<? extends Balancer> balancerClass;

    Official(Class<? extends Balancer> theClass) {
      this.balancerClass = theClass;
    }

    public Class<? extends Balancer> theClass() {
      return balancerClass;
    }

    public Balancer create(AlgorithmConfig config) {
      return Balancer.create(theClass(), config);
    }

    @Override
    public String alias() {
      return this.name();
    }

    @Override
    public String toString() {
      return alias();
    }

    public static Official ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Official.class, alias);
    }
  }
}
