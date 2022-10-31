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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.generator.ShufflePlanGenerator;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.cost.ClusterCost;

/**
 * A single-state hill-climbing algorithm. It discovers rebalance solution by tweaking the cluster
 * state multiple times, select the ideal tweak among the discovery. This process might take
 * multiple iterations, until no nicer tweak found.
 */
public class GreedyBalancer implements Balancer {

  public static final String SHUFFLE_PLAN_GENERATOR_MIN_STEP_CONFIG =
      "shuffle.plan.generator.min.step";
  public static final String SHUFFLE_PLAN_GENERATOR_MAX_STEP_CONFIG =
      "shuffle.plan.generator.max.step";
  public static final String ITERATION_CONFIG = "iteration";
  public static final Set<String> ALL_CONFIGS =
      Utils.constants(GreedyBalancer.class, name -> name.endsWith("CONFIG"));

  private final AlgorithmConfig config;
  private final int minStep;
  private final int maxStep;
  private final int iteration;

  public GreedyBalancer(AlgorithmConfig algorithmConfig) {
    this.config = algorithmConfig;
    minStep =
        config
            .algorithmConfig()
            .string(SHUFFLE_PLAN_GENERATOR_MIN_STEP_CONFIG)
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(1);
    maxStep =
        config
            .algorithmConfig()
            .string(SHUFFLE_PLAN_GENERATOR_MAX_STEP_CONFIG)
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(30);
    iteration =
        config
            .algorithmConfig()
            .string(ITERATION_CONFIG)
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(Integer.MAX_VALUE);
  }

  @Override
  public Optional<Plan> offer(
      ClusterInfo<Replica> currentClusterInfo, Map<Integer, Set<String>> brokerFolders) {
    final var planGenerator = new ShufflePlanGenerator(minStep, maxStep);
    final var metrics = config.metricSource().get();
    final var clusterCostFunction = config.clusterCostFunction();
    final var moveCostFunction = config.moveCostFunctions();

    final var loop = new AtomicInteger(iteration);
    final var start = System.currentTimeMillis();
    final var executionTime = config.executionTime().toMillis();
    Supplier<Boolean> moreRoom =
        () -> System.currentTimeMillis() - start < executionTime && loop.getAndDecrement() > 0;
    BiFunction<ClusterLogAllocation, ClusterCost, Optional<Balancer.Plan>> next =
        (currentAllocation, currentCost) ->
            planGenerator
                .generate(brokerFolders, currentAllocation)
                .takeWhile(ignored -> moreRoom.get())
                .map(
                    proposal -> {
                      var newClusterInfo =
                          ClusterInfo.update(
                              currentClusterInfo, tp -> proposal.rebalancePlan().replicas(tp));
                      return new Balancer.Plan(
                          proposal,
                          clusterCostFunction.clusterCost(newClusterInfo, metrics),
                          moveCostFunction.stream()
                              .map(cf -> cf.moveCost(currentClusterInfo, newClusterInfo, metrics))
                              .collect(Collectors.toList()));
                    })
                .filter(plan -> config.clusterConstraint().test(currentCost, plan.clusterCost()))
                .filter(plan -> config.movementConstraint().test(plan.moveCost()))
                .findFirst();
    var currentCost = clusterCostFunction.clusterCost(currentClusterInfo, metrics);
    var currentAllocation =
        ClusterLogAllocation.of(ClusterInfo.masked(currentClusterInfo, config.topicFilter()));
    var currentPlan = Optional.<Balancer.Plan>empty();
    while (true) {
      var newPlan = next.apply(currentAllocation, currentCost);
      if (newPlan.isEmpty()) break;
      currentPlan = newPlan;
      currentCost = currentPlan.get().clusterCost();
      currentAllocation = currentPlan.get().proposal().rebalancePlan();
    }
    return currentPlan;
  }
}
