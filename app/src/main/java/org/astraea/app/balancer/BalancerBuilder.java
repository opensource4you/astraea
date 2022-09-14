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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;

class BalancerBuilder {

  private RebalancePlanGenerator planGenerator;
  private Supplier<Map<Integer, Set<String>>> freshBrokerFolders;
  private Supplier<ClusterInfo<Replica>> freshLogAllocation;
  private final Supplier<ClusterBean> freshClusterBean = () -> ClusterBean.EMPTY;
  private RebalancePlanExecutor planExecutor;
  private HasClusterCost clusterCostFunction;
  private HasMoveCost moveCostFunction = HasMoveCost.EMPTY;
  private Predicate<ClusterCost> clusterConstraint = ignore -> true;
  private Predicate<MoveCost> movementConstraint = ignore -> true;
  private Predicate<String> topicFilter = ignore -> true;
  private int searchLimit = 3000;

  public BalancerBuilder usePlanGenerator(RebalancePlanGenerator generator, Admin admin) {
    return usePlanGenerator(generator, admin::brokerFolders, admin::clusterInfo);
  }

  public BalancerBuilder usePlanGenerator(
      RebalancePlanGenerator generator,
      Supplier<Map<Integer, Set<String>>> logFolderSupplier,
      Supplier<ClusterInfo<Replica>> clusterInfoSupplier) {
    this.planGenerator = generator;
    this.freshBrokerFolders = logFolderSupplier;
    this.freshLogAllocation = clusterInfoSupplier;
    return this;
  }

  public BalancerBuilder usePlanExecutor(RebalancePlanExecutor executor) {
    this.planExecutor = executor;
    return this;
  }

  public BalancerBuilder useClusterCost(HasClusterCost costFunction) {
    this.clusterCostFunction = costFunction;
    return this;
  }

  public BalancerBuilder useMoveCost(HasMoveCost costFunction) {
    this.moveCostFunction = costFunction;
    return this;
  }

  public BalancerBuilder useClusterConstraint(Predicate<ClusterCost> clusterConstraint) {
    this.clusterConstraint = clusterConstraint;
    return this;
  }

  public BalancerBuilder useMovementConstraint(Predicate<MoveCost> moveConstraint) {
    this.movementConstraint = moveConstraint;
    return this;
  }

  public BalancerBuilder useTopicFilter(Predicate<String> topicFilter) {
    // TODO: should this filter apply at generator or at cluster info?
    this.topicFilter = topicFilter;
    return this;
  }

  public BalancerBuilder searchLimit(int limit) {
    if (searchLimit <= 0)
      throw new IllegalArgumentException("invalid search limit: " + searchLimit);
    this.searchLimit = limit;
    return this;
  }

  public Balancer build() {
    // sanity check
    Objects.requireNonNull(this.planGenerator);
    Objects.requireNonNull(this.planExecutor);
    Objects.requireNonNull(this.freshBrokerFolders);
    Objects.requireNonNull(this.freshLogAllocation);
    Objects.requireNonNull(this.freshClusterBean);
    Objects.requireNonNull(this.clusterCostFunction);
    Objects.requireNonNull(this.moveCostFunction);
    Objects.requireNonNull(this.clusterConstraint);
    Objects.requireNonNull(this.movementConstraint);
    Objects.requireNonNull(this.topicFilter);

    return () -> {
      final var currentClusterInfo = freshLogAllocation.get();
      final var currentClusterBean = freshClusterBean.get();
      final var currentCostScore =
          clusterCostFunction.clusterCost(currentClusterInfo, currentClusterBean);

      return planGenerator
          .generate(freshBrokerFolders.get(), ClusterLogAllocation.of(currentClusterInfo))
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
                        currentClusterInfo, newClusterInfo, currentClusterBean),
                    planExecutor);
              })
          .filter(plan -> plan.clusterCost.value() < currentCostScore.value())
          .filter(plan -> clusterConstraint.test(plan.clusterCost))
          .filter(plan -> movementConstraint.test(plan.moveCost))
          .min(Comparator.comparing(plan -> plan.clusterCost.value()))
          .orElseThrow();
    };
  }
}
