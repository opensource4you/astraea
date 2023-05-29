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
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.BalancerConfigs;
import org.astraea.common.balancer.BalancerUtils;
import org.astraea.common.balancer.tweakers.ShuffleTweaker;
import org.astraea.common.cost.ClusterCost;

/** This algorithm proposes rebalance plan by tweaking the log allocation once. */
public class SingleStepBalancer implements Balancer {

  public static final String SHUFFLE_TWEAKER_MIN_STEP_CONFIG = "shuffle.tweaker.min.step";
  public static final String SHUFFLE_TWEAKER_MAX_STEP_CONFIG = "shuffle.tweaker.max.step";
  public static final String ITERATION_CONFIG = "iteration";
  public static final Set<String> ALL_CONFIGS =
      new TreeSet<>(
          Utils.constants(SingleStepBalancer.class, name -> name.endsWith("CONFIG"), String.class));

  @Override
  public Optional<Plan> offer(AlgorithmConfig config) {
    BalancerUtils.balancerConfigCheck(
        config.balancerConfig(),
        Set.of(
            BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX,
            BalancerConfigs.BALANCER_BROKER_BALANCING_MODE));

    final var minStep =
        config
            .balancerConfig()
            .string(SHUFFLE_TWEAKER_MIN_STEP_CONFIG)
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(1);
    final var maxStep =
        config
            .balancerConfig()
            .string(SHUFFLE_TWEAKER_MAX_STEP_CONFIG)
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(30);
    final var iteration =
        config
            .balancerConfig()
            .string(ITERATION_CONFIG)
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(Integer.MAX_VALUE);
    final var allowedTopics =
        config
            .balancerConfig()
            .regexString(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX)
            .map(Pattern::asMatchPredicate)
            .orElse((ignore) -> true);
    final var balancingMode =
        BalancerUtils.balancingMode(
            config.clusterInfo(),
            config
                .balancerConfig()
                .string(BalancerConfigs.BALANCER_BROKER_BALANCING_MODE)
                .orElse(""));
    final Predicate<Integer> isBalancing =
        id -> balancingMode.get(id) == BalancerUtils.BalancingModes.BALANCING;
    final Predicate<Integer> isClearing =
        id -> balancingMode.get(id) == BalancerUtils.BalancingModes.CLEAR;
    final var clearing =
        balancingMode.values().stream().anyMatch(i -> i == BalancerUtils.BalancingModes.CLEAR);
    BalancerUtils.verifyClearBrokerValidness(config.clusterInfo(), isClearing);

    final var currentClusterInfo =
        BalancerUtils.clearedCluster(config.clusterInfo(), isClearing, isBalancing);
    final var clusterBean = config.clusterBean();
    final var fixedReplicas =
        config
            .clusterInfo()
            .replicaStream()
            // if a topic is not allowed to move, it should be fixed.
            // if a topic is not allowed to move, but originally it located on a clearing broker, it
            // is ok to move.
            .filter(tpr -> !allowedTopics.test(tpr.topic()) && !isClearing.test(tpr.brokerId()))
            .collect(Collectors.toUnmodifiableSet());
    final var allocationTweaker =
        ShuffleTweaker.builder()
            .numberOfShuffle(() -> ThreadLocalRandom.current().nextInt(minStep, maxStep))
            .allowedReplicas(r -> !fixedReplicas.contains(r))
            .allowedBrokers(isBalancing)
            .build();
    final var moveCostFunction = config.moveCostFunction();

    final Function<ClusterInfo, ClusterCost> evaluateCost =
        (cluster) -> {
          final var filteredCluster =
              clearing ? ClusterInfo.builder(cluster).removeNodes(isClearing).build() : cluster;
          return config.clusterCostFunction().clusterCost(filteredCluster, clusterBean);
        };
    final var currentCost = evaluateCost.apply(currentClusterInfo);

    var start = System.currentTimeMillis();
    return allocationTweaker
        .generate(currentClusterInfo)
        .parallel()
        .limit(iteration)
        .takeWhile(ignored -> System.currentTimeMillis() - start <= config.timeout().toMillis())
        .filter(
            newAllocation ->
                !moveCostFunction
                    .moveCost(currentClusterInfo, newAllocation, clusterBean)
                    .overflow())
        .map(
            newAllocation ->
                new Plan(
                    config.clusterBean(),
                    config.clusterInfo(),
                    currentCost,
                    newAllocation,
                    evaluateCost.apply(newAllocation)))
        .filter(plan -> plan.proposalClusterCost().value() < currentCost.value())
        .min(Comparator.comparing(plan -> plan.proposalClusterCost().value()))
        .or(
            () -> {
              // With clearing, the implementation detail start search from a cleared state. It is
              // possible
              // that the start state is already the ideal answer. In this case, it is directly
              // returned.
              if (clearing
                  && currentCost.value() == 0.0
                  && !moveCostFunction
                      .moveCost(config.clusterInfo(), currentClusterInfo, clusterBean)
                      .overflow()) {
                return Optional.of(
                    new Plan(
                        config.clusterBean(),
                        config.clusterInfo(),
                        config.clusterCostFunction().clusterCost(config.clusterInfo(), clusterBean),
                        currentClusterInfo,
                        currentCost));
              }
              return Optional.empty();
            });
  }
}
