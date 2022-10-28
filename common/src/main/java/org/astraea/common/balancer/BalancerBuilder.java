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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.Configuration;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;

public class BalancerBuilder {

  private HasClusterCost clusterCostFunction;
  private List<HasMoveCost> moveCostFunction = List.of(HasMoveCost.EMPTY);
  private BiPredicate<ClusterCost, ClusterCost> clusterConstraint =
      (before, after) -> after.value() < before.value();
  private Predicate<List<MoveCost>> movementConstraint = ignore -> true;
  private int searchLimit = Integer.MAX_VALUE;
  private Duration executionTime = Duration.ofSeconds(3);
  private Supplier<ClusterBean> metricSource = () -> ClusterBean.EMPTY;
  private Configuration algorithmConfig = Configuration.of(Map.of());
  private Class<? extends Balancer> balancer = SingleStepBalancer.class;

  /**
   * Specify the cluster cost function to use. It implemented specific logic to evaluate if a
   * rebalance plan is worth using at certain performance/resource usage aspect
   *
   * @param costFunction the cost function for evaluating potential rebalance plan.
   * @return this
   */
  public BalancerBuilder clusterCost(HasClusterCost costFunction) {
    this.clusterCostFunction = Objects.requireNonNull(costFunction);
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
    this.moveCostFunction = Objects.requireNonNull(costFunction);
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
  public BalancerBuilder movementConstraint(Predicate<List<MoveCost>> moveConstraint) {
    this.movementConstraint = Objects.requireNonNull(moveConstraint);
    return this;
  }

  /**
   * Specify the maximum number of rebalance plans for evaluation. A higher number means searching &
   * evaluating more potential rebalance plans, which might lead to longer execution time.
   *
   * @deprecated The meaning of this parameter might change from algorithm to algorithm. Should use
   *     {@link BalancerBuilder#config} instead.
   * @param limit the maximum number of rebalance plan for evaluation.
   * @return this
   */
  @Deprecated
  public BalancerBuilder limit(int limit) {
    // TODO: get rid of this method. It proposes some kind of algorithm implementation details.
    this.searchLimit = Utils.requirePositive(limit);
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
   * @deprecated use {@link BalancerBuilder#balancer(Class)} instead
   * @return this builder
   */
  @Deprecated
  public BalancerBuilder greedy(boolean greedy) {
    // TODO: replaced by BalancerBuilder#algorithm
    return balancer(greedy ? GreedyBalancer.class : balancer);
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
   * The algorithm for rebalance plan search
   *
   * @param balancer the balancer implementation to use
   * @return this
   */
  public BalancerBuilder balancer(Class<? extends Balancer> balancer) {
    this.balancer = balancer;
    return this;
  }

  /**
   * @param configuration for {@link Balancer}
   * @return this
   */
  public BalancerBuilder config(Configuration configuration) {
    this.algorithmConfig = configuration;
    return this;
  }

  private AlgorithmConfig toConfig() {
    return new AlgorithmConfig() {
      @Override
      public HasClusterCost clusterCostFunction() {
        return clusterCostFunction;
      }

      @Override
      public List<HasMoveCost> moveCostFunctions() {
        return moveCostFunction;
      }

      @Override
      public BiPredicate<ClusterCost, ClusterCost> clusterConstraint() {
        return clusterConstraint;
      }

      @Override
      public Predicate<List<MoveCost>> movementConstraint() {
        return movementConstraint;
      }

      @Override
      public Duration executionTime() {
        return executionTime;
      }

      @Override
      public Supplier<ClusterBean> metricSource() {
        return metricSource;
      }

      private final Configuration prepared =
          Configuration.of(
              Stream.concat(
                      algorithmConfig.entrySet().stream(),
                      Stream.of(Map.entry("iteration", String.valueOf(searchLimit))))
                  .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)));

      @Override
      public Configuration algorithmConfig() {
        return prepared;
      }
    };
  }

  /**
   * @return a {@link Balancer} that will offer rebalance plan based on the implementation detail
   *     you specified.
   */
  public Balancer build() {
    return Utils.packException(
        () -> balancer.getConstructor(AlgorithmConfig.class).newInstance(toConfig()));
  }
}
