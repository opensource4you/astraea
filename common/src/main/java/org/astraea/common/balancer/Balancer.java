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
import org.astraea.common.EnumInfo;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.MoveCost;
import org.astraea.common.cost.NoSufficientMetricsException;

public interface Balancer {

  /**
   * Execute {@link Balancer#offer(ClusterInfo, Duration)}. Retry the plan generation if a {@link
   * NoSufficientMetricsException} exception occurred.
   */
  default Plan retryOffer(ClusterInfo<Replica> currentClusterInfo, Duration timeout) {
    final var timeoutMs = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < timeoutMs) {
      try {
        return offer(currentClusterInfo, Duration.ofMillis(timeoutMs - System.currentTimeMillis()));
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
  Plan offer(ClusterInfo<Replica> currentClusterInfo, Duration timeout);

  @SuppressWarnings("unchecked")
  static Balancer create(String classpath, AlgorithmConfig config) {
    var theClass = Utils.packException(() -> Class.forName(classpath));
    if (Balancer.class.isAssignableFrom(theClass)) {
      return create(((Class<? extends Balancer>) theClass), config);
    } else
      throw new IllegalArgumentException("Given class is not a balancer: " + theClass.getName());
  }

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

    final ClusterInfo<Replica> proposal;
    final ClusterCost proposalClusterCost;
    final MoveCost moveCost;

    public ClusterInfo<Replica> proposal() {
      return proposal;
    }

    /** The {@link ClusterCost} score of the proposed new allocation. */
    public ClusterCost proposalClusterCost() {
      return proposalClusterCost;
    }

    public MoveCost moveCost() {
      return moveCost;
    }

    public Solution(
        ClusterCost proposalClusterCost, MoveCost moveCost, ClusterInfo<Replica> proposal) {
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
