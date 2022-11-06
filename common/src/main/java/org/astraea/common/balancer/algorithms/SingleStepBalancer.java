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

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.BalancerProgressReport;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.balancer.tweakers.ShuffleTweaker;

/** This algorithm proposes rebalance plan by tweaking the log allocation once. */
public class SingleStepBalancer implements Balancer {

  public static final String SHUFFLE_PLAN_GENERATOR_MIN_STEP_CONFIG =
      "shuffle.plan.generator.min.step";
  public static final String SHUFFLE_PLAN_GENERATOR_MAX_STEP_CONFIG =
      "shuffle.plan.generator.max.step";
  public static final String ITERATION_CONFIG = "iteration";
  public static final Set<String> ALL_CONFIGS =
      new TreeSet<>(Utils.constants(SingleStepBalancer.class, name -> name.endsWith("CONFIG")));

  private final AlgorithmConfig config;
  private final int minStep;
  private final int maxStep;
  private final int iteration;

  public SingleStepBalancer(AlgorithmConfig algorithmConfig) {
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
  public Optional<Balancer.Plan> offer(
      ClusterInfo<Replica> currentClusterInfo,
      Map<Integer, Set<String>> brokerFolders,
      BalancerProgressReport progressReport) {
    final var allocationTweaker = new ShuffleTweaker(minStep, maxStep);
    final var currentClusterBean = config.metricSource().get();
    final var clusterCostFunction = config.clusterCostFunction();
    final var moveCostFunction = config.moveCostFunctions();
    final var currentCost =
        config.clusterCostFunction().clusterCost(currentClusterInfo, currentClusterBean);
    final var generatorClusterInfo = ClusterInfo.masked(currentClusterInfo, config.topicFilter());

    var start = System.currentTimeMillis();
    var executionTime = config.executionTime().toMillis();
    return allocationTweaker
        .generate(brokerFolders, ClusterLogAllocation.of(generatorClusterInfo))
        .parallel()
        .limit(iteration)
        .takeWhile(ignored -> System.currentTimeMillis() - start <= executionTime)
        .map(
            newAllocation -> {
              var newClusterInfo = ClusterInfo.update(currentClusterInfo, newAllocation::replicas);
              return new Balancer.Plan(
                  newAllocation,
                  clusterCostFunction.clusterCost(newClusterInfo, currentClusterBean),
                  moveCostFunction.stream()
                      .map(
                          cf -> cf.moveCost(currentClusterInfo, newClusterInfo, currentClusterBean))
                      .collect(Collectors.toList()));
            })
        .filter(plan -> config.clusterConstraint().test(currentCost, plan.clusterCost()))
        .filter(plan -> config.movementConstraint().test(plan.moveCost()))
        .peek(plan -> progressReport.cost(System.currentTimeMillis(), plan.clusterCost().value()))
        .min(Comparator.comparing(plan -> plan.clusterCost().value()));
  }
}
