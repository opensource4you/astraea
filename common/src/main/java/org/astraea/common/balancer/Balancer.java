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
import java.util.Optional;
import org.astraea.common.Configuration;
import org.astraea.common.EnumInfo;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.MoveCost;
import org.astraea.common.cost.NoSufficientMetricsException;

public interface Balancer {

  /**
   * Execute {@link Balancer#offer(ClusterInfo, ClusterBean, Duration, AlgorithmConfig)}. Retry the
   * plan generation if a {@link NoSufficientMetricsException} exception occurred.
   */
  default Plan retryOffer(
      ClusterInfo currentClusterInfo, Duration timeout, AlgorithmConfig config) {
    final var timeoutMs = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < timeoutMs) {
      try {
        return offer(
            currentClusterInfo,
            config.metricSource().get(),
            Duration.ofMillis(timeoutMs - System.currentTimeMillis()),
            config);
      } catch (NoSufficientMetricsException e) {
        e.printStackTrace();
        var remainTimeout = timeoutMs - System.currentTimeMillis();
        var waitMs = e.suggestedWait().toMillis();
        if (remainTimeout > waitMs) {
          Utils.sleep(Duration.ofMillis(waitMs));
        } else {
          // This suggested wait time will definitely time out after we woke up
          throw new RuntimeException(
              "Execution time will exceeded, "
                  + "remain: "
                  + remainTimeout
                  + "ms, suggestedWait: "
                  + waitMs
                  + "ms.",
              e);
        }
      }
    }
    throw new RuntimeException("Execution time exceeded: " + timeoutMs);
  }

  /**
   * @return a rebalance plan
   */
  Plan offer(
      ClusterInfo currentClusterInfo,
      ClusterBean clusterBean,
      Duration timeout,
      AlgorithmConfig config);

  class Plan {
    final ClusterCost initialClusterCost;
    final Solution solution;

    /**
     * The {@link ClusterCost} score of the original {@link ClusterInfo} when this plan is start
     * generating.
     */
    public ClusterCost initialClusterCost() {
      return initialClusterCost;
    }

    public Optional<Solution> solution() {
      return Optional.ofNullable(solution);
    }

    public Plan(ClusterCost initialClusterCost) {
      this(initialClusterCost, null);
    }

    public Plan(ClusterCost initialClusterCost, Solution solution) {
      this.initialClusterCost = initialClusterCost;
      this.solution = solution;
    }
  }

  class Solution {

    final ClusterInfo proposal;
    final ClusterCost proposalClusterCost;
    final MoveCost moveCost;

    public ClusterInfo proposal() {
      return proposal;
    }

    /** The {@link ClusterCost} score of the proposed new allocation. */
    public ClusterCost proposalClusterCost() {
      return proposalClusterCost;
    }

    public MoveCost moveCost() {
      return moveCost;
    }

    public Solution(ClusterCost proposalClusterCost, MoveCost moveCost, ClusterInfo proposal) {
      this.proposal = proposal;
      this.proposalClusterCost = proposalClusterCost;
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

    public Balancer create(Configuration config) {
      return Utils.construct(theClass(), config);
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
