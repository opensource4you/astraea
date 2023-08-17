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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.metrics.ClusterBean;

/**
 * The generic algorithm parameter for resolving the Kafka rebalance problem.
 *
 * @param executionId a String indicate the name of this execution. This information is used for
 *     debug and logging usage.
 * @param clusterCostFunction the cluster cost function for this problem.
 * @param moveCostFunction the movement cost functions for this problem
 * @param balancerConfig the configuration of this balancer run
 * @param clusterInfo the initial cluster state of this optimization problem
 * @param clusterBean the metrics of the associated cluster and optimization problem
 * @param timeout the execution limit of this optimization problem
 */
public record AlgorithmConfig(
    String executionId,
    HasClusterCost clusterCostFunction,
    HasMoveCost moveCostFunction,
    Configuration balancerConfig,
    ClusterInfo clusterInfo,
    ClusterBean clusterBean,
    Duration timeout) {

  public static Builder builder() {
    return new Builder(null);
  }

  public static Builder builder(AlgorithmConfig config) {
    return new Builder(config);
  }

  public static class Builder {

    private String executionId = "noname-" + UUID.randomUUID();
    private HasClusterCost clusterCostFunction;
    private HasMoveCost moveCostFunction = HasMoveCost.EMPTY;
    private final Map<String, String> balancerConfig = new HashMap<>();

    private ClusterInfo clusterInfo;
    private ClusterBean clusterBean = ClusterBean.EMPTY;
    private Duration timeout = Duration.ofSeconds(3);

    private Builder(AlgorithmConfig config) {
      if (config != null) {
        this.executionId = config.executionId();
        this.clusterCostFunction = config.clusterCostFunction();
        this.moveCostFunction = config.moveCostFunction();
        this.balancerConfig.putAll(config.balancerConfig().raw());
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
     * Put a set of key/value configuration for balancer.
     *
     * @return this
     */
    public Builder configs(Map<String, String> config) {
      this.balancerConfig.putAll(config);
      return this;
    }

    /**
     * Put a key/value configuration for balancer.
     *
     * @return this
     */
    public Builder config(String key, String value) {
      this.balancerConfig.put(key, value);
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
      return new AlgorithmConfig(
          executionId,
          clusterCostFunction,
          moveCostFunction,
          new Configuration(balancerConfig),
          clusterInfo,
          clusterBean,
          timeout);
    }
  }
}
