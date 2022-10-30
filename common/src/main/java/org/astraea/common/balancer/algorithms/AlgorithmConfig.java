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
package org.astraea.common.balancer.algorithms;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.Configuration;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;

/** The generic algorithm parameter for resolving the Kafka rebalance problem. */
public interface AlgorithmConfig {

  static Builder builder() {
    return new Builder();
  }

  /** @return the cluster cost function for this problem. */
  HasClusterCost clusterCostFunction();

  /** @return the movement cost functions for this problem */
  List<HasMoveCost> moveCostFunctions();

  /** @return the cluster cost constraint that must be complied with by the algorithm solution */
  BiPredicate<ClusterCost, ClusterCost> clusterConstraint();

  /** @return the movement constraint that must be complied with by the algorithm solution */
  Predicate<List<MoveCost>> movementConstraint();

  /** @return the limit of algorithm execution time */
  Duration executionTime();

  /** @return a {@link Predicate} that can indicate which topic is eligible for rebalance. */
  Predicate<String> topicFilter();

  /** @return a {@link Supplier} which offer the fresh metrics of the target cluster */
  Supplier<ClusterBean> metricSource();

  /** @return the algorithm implementation specific parameters */
  Configuration algorithmConfig();

  class Builder {

    private HasClusterCost clusterCostFunction;
    private List<HasMoveCost> moveCostFunction = List.of(HasMoveCost.EMPTY);
    private BiPredicate<ClusterCost, ClusterCost> clusterConstraint =
        (before, after) -> after.value() < before.value();
    private Predicate<List<MoveCost>> movementConstraint = ignore -> true;
    private Duration executionTime = Duration.ofSeconds(3);
    private Supplier<ClusterBean> metricSource = () -> ClusterBean.EMPTY;
    private Predicate<String> topicFilter = ignore -> true;
    private final Map<String, String> config = new HashMap<>();

    /**
     * Specify the cluster cost function to use. It implemented specific logic to evaluate if a
     * rebalance plan is worth using at certain performance/resource usage aspect
     *
     * @param costFunction the cost function for evaluating potential rebalance plan.
     * @return this
     */
    public Builder clusterCost(HasClusterCost costFunction) {
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
    public Builder moveCost(List<HasMoveCost> costFunction) {
      this.moveCostFunction = Objects.requireNonNull(costFunction);
      return this;
    }

    /**
     * Specify the cluster cost constraint for any rebalance plan.
     *
     * @param clusterConstraint a {@link BiPredicate} to determine if the rebalance result is
     *     acceptable(in terms of performance/resource consideration). The first argument is the
     *     {@link ClusterCost} of current cluster, and the second argument is the {@link
     *     ClusterCost} of the proposed new cluster.
     * @return this
     */
    public Builder clusterConstraint(BiPredicate<ClusterCost, ClusterCost> clusterConstraint) {
      this.clusterConstraint = clusterConstraint;
      return this;
    }

    /**
     * Specify the movement cost constraint for any rebalance plan.
     *
     * @param moveConstraint a {@link Predicate} to determine if the rebalance result is
     *     acceptable(in terms of the ongoing cost caused by execute this rebalance plan).
     * @return this
     */
    public Builder movementConstraint(Predicate<List<MoveCost>> moveConstraint) {
      this.movementConstraint = Objects.requireNonNull(moveConstraint);
      return this;
    }

    /**
     * Specify the maximum number of rebalance plans for evaluation. A higher number means searching
     * & evaluating more potential rebalance plans, which might lead to longer execution time.
     *
     * @deprecated The meaning of this parameter might change from algorithm to algorithm. Should
     *     use {@link Builder#config} instead.
     * @param limit the maximum number of rebalance plan for evaluation.
     * @return this
     */
    @Deprecated
    public Builder limit(int limit) {
      // TODO: get rid of this method. It proposes some kind of algorithm implementation details.
      this.config("iteration", String.valueOf(Utils.requirePositive(limit)));
      return this;
    }

    /**
     * @param limit the execution time of searching best plan.
     * @return this
     */
    public Builder limit(Duration limit) {
      this.executionTime = limit;
      return this;
    }

    /**
     * Specify the source of bean metrics. The default supplier return {@link ClusterBean#EMPTY}
     * only, which means any cost function that interacts with metrics won't work. To use a cost
     * function with metrics requirement, one must specify the concrete bean metric source by
     * invoking this method.
     *
     * @param metricSource a {@link Supplier} offers the newest {@link ClusterBean} of target
     *     cluster
     * @return this
     */
    public Builder metricSource(Supplier<ClusterBean> metricSource) {
      this.metricSource = metricSource;
      return this;
    }

    /**
     * Specify the topics that are eligible for rebalance.
     *
     * @param topicFilter the {@link Predicate} what can indicate which topic is eligible for
     *     rebalance.
     * @return this
     */
    public Builder topicFilter(Predicate<String> topicFilter) {
      this.topicFilter = topicFilter;
      return this;
    }

    /**
     * @param configuration for {@link Balancer}
     * @return this
     */
    public Builder config(Configuration configuration) {
      configuration.entrySet().forEach((e) -> this.config(e.getKey(), e.getValue()));
      return this;
    }

    /**
     * @param key for {@link Balancer} implementation specific config
     * @param value for {@link Balancer} implementation specific config
     * @return this
     */
    public Builder config(String key, String value) {
      this.config.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
      return this;
    }

    public AlgorithmConfig build() {
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
        public Predicate<String> topicFilter() {
          return topicFilter;
        }

        @Override
        public Supplier<ClusterBean> metricSource() {
          return metricSource;
        }

        private final Configuration prepared = Configuration.of(Map.copyOf(config));

        @Override
        public Configuration algorithmConfig() {
          return prepared;
        }
      };
    }
  }
}
