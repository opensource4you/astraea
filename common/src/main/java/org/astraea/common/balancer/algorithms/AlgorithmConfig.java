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

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;

/** The generic algorithm parameter for resolving the Kafka rebalance problem. */
public interface AlgorithmConfig {

  static Builder builder() {
    return new Builder(null);
  }

  static Builder builder(AlgorithmConfig config) {
    return new Builder(config);
  }

  /**
   * @return a String indicate the name of this execution. This information is used for debug and
   *     logging usage.
   */
  String executionId();

  /**
   * @return the cluster cost function for this problem.
   */
  HasClusterCost clusterCostFunction();

  /**
   * @return the movement cost functions for this problem
   */
  List<HasMoveCost> moveCostFunctions();

  /**
   * @return the cluster cost constraint that must be complied with by the algorithm solution
   */
  BiPredicate<ClusterCost, ClusterCost> clusterConstraint();

  /**
   * @return the movement constraint that must be complied with by the algorithm solution
   */
  Predicate<List<MoveCost>> movementConstraint();

  /**
   * @return a {@link Predicate} that can indicate which topic is eligible for rebalance.
   */
  Predicate<String> topicFilter();

  /**
   * @return a {@link Supplier} which offer the fresh metrics of the target cluster
   */
  Supplier<ClusterBean> metricSource();

  /**
   * @return the algorithm implementation specific parameters
   */
  Configuration config();

  class Builder {

    private String executionId = "noname-" + UUID.randomUUID();
    private HasClusterCost clusterCostFunction;
    private List<HasMoveCost> moveCostFunctions = List.of(HasMoveCost.EMPTY);
    private BiPredicate<ClusterCost, ClusterCost> clusterConstraint =
        (before, after) -> after.value() < before.value();
    private Predicate<List<MoveCost>> movementConstraint = ignore -> true;
    private Supplier<ClusterBean> metricSource = () -> ClusterBean.EMPTY;
    private Predicate<String> topicFilter = ignore -> true;
    private Configuration config = Configuration.EMPTY;

    private Builder(AlgorithmConfig config) {
      if (config != null) {
        this.executionId = config.executionId();
        this.clusterCostFunction = config.clusterCostFunction();
        this.moveCostFunctions = config.moveCostFunctions();
        this.clusterConstraint = config.clusterConstraint();
        this.movementConstraint = config.movementConstraint();
        this.metricSource = config.metricSource();
        this.topicFilter = config.topicFilter();
        this.config = config.config();
      }
    }

    /**
     * Set a String that represents the execution of this algorithm. This information is typically
     * used for debugging and logging usage.
     *
     * @return this
     */
    public Builder executionId(String id) {
      this.executionId = id;
      return this;
    }

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
      this.moveCostFunctions = Objects.requireNonNull(costFunction);
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
     * @param config for {@link Balancer}
     * @return this
     */
    public Builder config(Configuration config) {
      this.config = config;
      return this;
    }

    public AlgorithmConfig build() {
      Objects.requireNonNull(clusterCostFunction);

      return new AlgorithmConfig() {
        @Override
        public String executionId() {
          return executionId;
        }

        @Override
        public HasClusterCost clusterCostFunction() {
          return clusterCostFunction;
        }

        @Override
        public List<HasMoveCost> moveCostFunctions() {
          return moveCostFunctions;
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
        public Predicate<String> topicFilter() {
          return topicFilter;
        }

        @Override
        public Supplier<ClusterBean> metricSource() {
          return metricSource;
        }

        @Override
        public Configuration config() {
          return config;
        }
      };
    }
  }
}
