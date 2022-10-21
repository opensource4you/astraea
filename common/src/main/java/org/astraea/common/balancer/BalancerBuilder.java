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

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.balancer.generator.RebalancePlanGenerator;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;

public class BalancerBuilder {

  private RebalancePlanGenerator planGenerator;
  private HasClusterCost clusterCostFunction;
  private List<HasMoveCost> moveCostFunction = List.of(HasMoveCost.EMPTY);
  private BiPredicate<ClusterCost, ClusterCost> clusterConstraint =
      (before, after) -> after.value() < before.value();
  private Map<String, Predicate<MoveCost>> movementConstraint = Map.of("", ignore -> true);
  private int searchLimit = Integer.MAX_VALUE;
  private Duration executionTime = Duration.ofSeconds(3);
  private Supplier<ClusterBean> metricSource = () -> ClusterBean.EMPTY;

  private boolean greedy = false;

  /**
   * Specify the {@link RebalancePlanGenerator} for potential rebalance plan generation.
   *
   * @param generator the generator instance.
   * @return this
   */
  public BalancerBuilder planGenerator(RebalancePlanGenerator generator) {
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
  public BalancerBuilder clusterCost(HasClusterCost costFunction) {
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
  public BalancerBuilder moveCost(List<HasMoveCost> costFunction) {
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
  public BalancerBuilder clusterConstraint(
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
  public BalancerBuilder movementConstraint(Map<String, Predicate<MoveCost>> moveConstraint) {
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
  public BalancerBuilder limit(int limit) {
    if (limit <= 0) throw new IllegalArgumentException("invalid search limit: " + limit);
    this.searchLimit = limit;
    return this;
  }

  /**
   * @param limit the execution time of searching best plan.
   * @return this
   */
  public BalancerBuilder limit(Duration limit) {
    this.executionTime = limit;
    return this;
  }

  /**
   * greedy mode means the balancer will use the first better plan to find out next better plan. The
   * advantage is that balancer always try to find a "better" plan. However, it can't generate the
   * best plan if the "first"/"second"/... better plan is not good enough.
   *
   * @return this builder
   */
  public BalancerBuilder greedy(boolean greedy) {
    this.greedy = greedy;
    return this;
  }

  /**
   * Specify the source of bean metrics. The default supplier return {@link ClusterBean#EMPTY} only,
   * which means any cost function that interacts with metrics won't work. To use a cost function
   * with metrics requirement, one must specify the concrete bean metric source by invoking this
   * method.
   *
   * @param metricSource a {@link Supplier} offers the newest {@link ClusterBean} of target cluster
   * @return this
   */
  public BalancerBuilder metricSource(Supplier<ClusterBean> metricSource) {
    this.metricSource = metricSource;
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

    if (greedy) return buildGreedy();
    return buildNormal();
  }

  private Balancer buildNormal() {
    return (currentClusterInfo, topicFilter, brokerFolders) -> {
      final var currentClusterBean = metricSource.get();
      final var currentCost =
          clusterCostFunction.clusterCost(currentClusterInfo, currentClusterBean);
      final var generatorClusterInfo = ClusterInfo.masked(currentClusterInfo, topicFilter);

      var start = System.currentTimeMillis();
      return planGenerator
          .generate(brokerFolders, ClusterLogAllocation.of(generatorClusterInfo))
          .parallel()
          .limit(searchLimit)
          .takeWhile(ignored -> System.currentTimeMillis() - start <= executionTime.toMillis())
          .map(
              proposal -> {
                var newClusterInfo =
                    ClusterInfo.update(
                        currentClusterInfo, tp -> proposal.rebalancePlan().logPlacements(tp));
                return new Balancer.Plan(
                    proposal,
                    clusterCostFunction.clusterCost(newClusterInfo, currentClusterBean),
                    moveCostFunction.stream()
                        .map(
                            cf ->
                                cf.moveCost(currentClusterInfo, newClusterInfo, currentClusterBean))
                        .collect(Collectors.toList()));
              })
          .filter(plan -> clusterConstraint.test(currentCost, plan.clusterCost))
          .filter(
              plan ->
                  plan.moveCost().stream()
                      .allMatch(
                          moveCost ->
                              movementConstraint
                                  .getOrDefault(moveCost.name(), (ignore) -> true)
                                  .test(moveCost)))
          .min(Comparator.comparing(plan -> plan.clusterCost.value()));
    };
  }

  private Balancer buildGreedy() {
    return (originClusterInfo, topicFilter, brokerFolders) -> {
      final var metrics = metricSource.get();
      final var loop = new AtomicInteger(searchLimit);
      final var start = System.currentTimeMillis();
      Supplier<Boolean> moreRoom =
          () ->
              System.currentTimeMillis() - start < executionTime.toMillis()
                  && loop.getAndDecrement() > 0;
      BiFunction<ClusterLogAllocation, ClusterCost, Optional<Balancer.Plan>> next =
          (currentAllocation, currentCost) ->
              planGenerator
                  .generate(brokerFolders, currentAllocation)
                  .takeWhile(ignored -> moreRoom.get())
                  .map(
                      proposal -> {
                        var newClusterInfo =
                            ClusterInfo.update(
                                originClusterInfo,
                                tp -> proposal.rebalancePlan().logPlacements(tp));
                        return new Balancer.Plan(
                            proposal,
                            clusterCostFunction.clusterCost(newClusterInfo, metrics),
                            moveCostFunction.stream()
                                .map(cf -> cf.moveCost(originClusterInfo, newClusterInfo, metrics))
                                .collect(Collectors.toList()));
                      })
                  .filter(plan -> clusterConstraint.test(currentCost, plan.clusterCost))
                  .filter(
                      plan ->
                          plan.moveCost().stream()
                              .allMatch(
                                  moveCost ->
                                      movementConstraint
                                          .getOrDefault(moveCost.name(), (ignore) -> true)
                                          .test(moveCost)))
                  .findFirst();
      var currentCost = clusterCostFunction.clusterCost(originClusterInfo, metrics);
      var currentAllocation =
          ClusterLogAllocation.of(ClusterInfo.masked(originClusterInfo, topicFilter));
      var currentPlan = Optional.<Balancer.Plan>empty();
      while (true) {
        var newPlan = next.apply(currentAllocation, currentCost);
        if (newPlan.isEmpty()) break;
        currentPlan = newPlan;
        currentCost = currentPlan.get().clusterCost;
        currentAllocation = currentPlan.get().proposal().rebalancePlan();
      }
      return currentPlan;
    };
  }
}
