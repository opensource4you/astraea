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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.generator.ShufflePlanGenerator;
import org.astraea.common.balancer.log.ClusterLogAllocation;

/** This algorithm proposes rebalance plan by tweaking the log allocation once. */
public class SingleStepAlgorithm extends Balancer {

  private final int minStep;
  private final int maxStep;
  private final int iteration;

  public SingleStepAlgorithm(AlgorithmConfig algorithmConfig) {
    super(algorithmConfig);

    minStep =
        config()
            .algorithmConfig()
            .string("min.step")
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(5);
    maxStep =
        config()
            .algorithmConfig()
            .string("max.step")
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(20);
    iteration =
        config()
            .algorithmConfig()
            .string("iteration")
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(Integer.MAX_VALUE);
  }

  @Override
  public Optional<Balancer.Plan> offer(
      ClusterInfo<Replica> currentClusterInfo,
      Predicate<String> topicFilter,
      Map<Integer, Set<String>> brokerFolders) {
    final var planGenerator = new ShufflePlanGenerator(minStep, maxStep);
    final var currentClusterBean = config().metricSource().get();
    final var clusterCostFunction = config().clusterCostFunction();
    final var moveCostFunction = config().moveCostFunctions();
    final var currentCost =
        config().clusterCostFunction().clusterCost(currentClusterInfo, currentClusterBean);
    final var generatorClusterInfo = ClusterInfo.masked(currentClusterInfo, topicFilter);

    var start = System.currentTimeMillis();
    var executionTime = config().executionTime().toMillis();
    return planGenerator
        .generate(brokerFolders, ClusterLogAllocation.of(generatorClusterInfo))
        .parallel()
        .limit(iteration)
        .takeWhile(ignored -> System.currentTimeMillis() - start <= executionTime)
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
                          cf -> cf.moveCost(currentClusterInfo, newClusterInfo, currentClusterBean))
                      .collect(Collectors.toList()));
            })
        .filter(plan -> config().clusterConstraint().test(currentCost, plan.clusterCost()))
        .filter(plan -> config().movementConstraint().test(plan.moveCost()))
        .min(Comparator.comparing(plan -> plan.clusterCost().value()));
  }
}
