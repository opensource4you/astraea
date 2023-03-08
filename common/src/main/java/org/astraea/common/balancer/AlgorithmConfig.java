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
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
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
  HasMoveCost moveCostFunction();

  /**
   * @return the cluster cost constraint that must be complied with by the algorithm solution
   */
  BiPredicate<ClusterCost, ClusterCost> clusterConstraint();

  /**
   * @return the movement constraint that must be complied with by the algorithm solution
   */
  Predicate<MoveCost> movementConstraint();

  /**
   * @return a {@link Predicate} that can indicate which topic is eligible for rebalance.
   */
  Predicate<String> topicFilter();

  /**
   * @return the initial cluster state of this optimization problem
   */
  ClusterInfo clusterInfo();

  /**
   * @return the metrics of the associated cluster and optimization problem
   */
  ClusterBean clusterBean();

  /**
   * @return the execution limit of this optimization problem
   */
  Duration timeout();

  class Builder {

    private String executionId = "noname-" + UUID.randomUUID();
    private HasClusterCost clusterCostFunction;
    private HasMoveCost moveCostFunction = HasMoveCost.EMPTY;
    private BiPredicate<ClusterCost, ClusterCost> clusterConstraint =
        (before, after) -> after.value() < before.value();
    private Predicate<MoveCost> movementConstraint = moveCost -> !moveCost.overflow();
    private Predicate<String> topicFilter = ignore -> true;

    private ClusterInfo clusterInfo;
    private ClusterBean clusterBean = ClusterBean.EMPTY;
    private Duration timeout = Duration.ofSeconds(3);

    private Builder(AlgorithmConfig config) {
      if (config != null) {
        this.executionId = config.executionId();
        this.clusterCostFunction = config.clusterCostFunction();
        this.moveCostFunction = config.moveCostFunction();
        this.clusterConstraint = config.clusterConstraint();
        this.movementConstraint = config.movementConstraint();
        this.topicFilter = config.topicFilter();
        this.clusterInfo = config.clusterInfo();
        this.clusterBean = config.clusterBean();
        this.timeout = config.timeout();
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
    public Builder moveCost(HasMoveCost costFunction) {
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
     * Specify the initial cluster state of this optimization problem
     *
     * @return this
     */
    public Builder clusterInfo(ClusterInfo clusterInfo) {
      this.clusterInfo = clusterInfo;
      return this;
    }

    /**
     * Specify the metrics of the associated Kafka cluster in this optimization problem
     *
     * @return this
     */
    public Builder clusterBean(ClusterBean clusterBean) {
      this.clusterBean = clusterBean;
      return this;
    }

    /**
     * Specify the execution timeout of this optimization problem
     *
     * @return this
     */
    public Builder timeout(Duration timeout) {
      this.timeout = timeout;
      return this;
    }

    public AlgorithmConfig build() {

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
        public HasMoveCost moveCostFunction() {
          return moveCostFunction;
        }

        @Override
        public BiPredicate<ClusterCost, ClusterCost> clusterConstraint() {
          return clusterConstraint;
        }

        @Override
        public Predicate<MoveCost> movementConstraint() {
          return movementConstraint;
        }

        @Override
        public Predicate<String> topicFilter() {
          return topicFilter;
        }

        @Override
        public ClusterInfo clusterInfo() {
          return clusterInfo;
        }

        @Override
        public ClusterBean clusterBean() {
          return clusterBean;
        }

        @Override
        public Duration timeout() {
          return timeout;
        }
      };
    }
  }
}
