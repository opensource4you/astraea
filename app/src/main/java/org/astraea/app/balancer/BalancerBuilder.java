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
package org.astraea.app.balancer;

import java.util.Comparator;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;

class BalancerBuilder {

  private RebalancePlanGenerator planGenerator;
  private HasClusterCost clusterCostFunction;
  private HasMoveCost moveCostFunction = HasMoveCost.EMPTY;
  private BiPredicate<ClusterCost, ClusterCost> clusterConstraint =
      (before, after) -> after.value() < before.value();
  private Predicate<MoveCost> movementConstraint = ignore -> true;
  private int searchLimit = 3000;

  /**
   * Specify the {@link RebalancePlanGenerator} for potential rebalance plan generation.
   *
   * @param generator the generator instance.
   * @return this
   */
  public BalancerBuilder usePlanGenerator(RebalancePlanGenerator generator) {
    this.planGenerator = generator;
    return this;
  }

  /**
   * Specify the cluster cost function to use. It implemented specific logic to evaluate if a
   * rebalance plan is worth using at certain performance/resource usage aspect
   *
   * @param costFunction the cost function for evaluating potential rebalance plan.
   * @return this
   */
  public BalancerBuilder useClusterCost(HasClusterCost costFunction) {
    this.clusterCostFunction = costFunction;
    return this;
  }

  /**
   * Specify the movement cost function to use. It implemented specific logic to evaluate the
   * performance/resource impact against the cluster if we adopt a given rebalance plan.
   *
   * @param costFunction the cost function for evaluating the impact of a rebalance plan to a
   *     cluster.
   * @return this
   */
  public BalancerBuilder useMoveCost(HasMoveCost costFunction) {
    this.moveCostFunction = costFunction;
    return this;
  }

  /**
   * Specify the cluster cost constraint for any rebalance plan.
   *
   * @param clusterConstraint a {@link BiPredicate} to determine if the rebalance result is
   *     acceptable(in terms of performance/resource consideration). The first argument is the
   *     {@link ClusterCost} of current cluster, and the second argument is the {@link ClusterCost}
   *     of the proposed new cluster.
   * @return this
   */
  public BalancerBuilder useClusterConstraint(
      BiPredicate<ClusterCost, ClusterCost> clusterConstraint) {
    this.clusterConstraint = clusterConstraint;
    return this;
  }

  /**
   * Specify the movement cost constraint for any rebalance plan.
   *
   * @param moveConstraint a {@link Predicate} to determine if the rebalance result is acceptable(in
   *     terms of the ongoing cost caused by execute this rebalance plan).
   * @return this
   */
  public BalancerBuilder useMovementConstraint(Predicate<MoveCost> moveConstraint) {
    this.movementConstraint = moveConstraint;
    return this;
  }

  /**
   * Specify the maximum number of rebalance plans for evaluation. A higher number means searching &
   * evaluating more potential rebalance plans, which might lead to longer execution time.
   *
   * @param limit the maximum number of rebalance plan for evaluation.
   * @return this
   */
  public BalancerBuilder searches(int limit) {
    if (searchLimit <= 0)
      throw new IllegalArgumentException("invalid search limit: " + searchLimit);
    this.searchLimit = limit;
    return this;
  }

  /**
   * @return a {@link Balancer} that will offer rebalance plan based on the implementation detail
   *     you specified.
   */
  public Balancer build() {
    // sanity check
    Objects.requireNonNull(this.planGenerator);
    Objects.requireNonNull(this.clusterCostFunction);
    Objects.requireNonNull(this.moveCostFunction);
    Objects.requireNonNull(this.movementConstraint);

    return (currentClusterInfo, topicFilter, brokerFolders) -> {
      final var currentClusterBean = ClusterBean.EMPTY;
      final var currentCost =
          clusterCostFunction.clusterCost(currentClusterInfo, currentClusterBean);
      final var generatorClusterInfo = ClusterInfo.masked(currentClusterInfo, topicFilter);

      return planGenerator
          .generate(brokerFolders, ClusterLogAllocation.of(generatorClusterInfo))
          .parallel()
          .limit(searchLimit)
          .map(
              proposal -> {
                var newClusterInfo =
                    BalancerUtils.update(currentClusterInfo, proposal.rebalancePlan());
                return new Balancer.Plan(
                    proposal,
                    clusterCostFunction.clusterCost(newClusterInfo, currentClusterBean),
                    moveCostFunction.moveCost(
                        currentClusterInfo, newClusterInfo, currentClusterBean));
              })
          .filter(plan -> clusterConstraint.test(currentCost, plan.clusterCost))
          .filter(plan -> movementConstraint.test(plan.moveCost))
          .min(Comparator.comparing(plan -> plan.clusterCost.value()))
          .orElseThrow();
    };
  }
}
