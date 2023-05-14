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
import java.util.regex.Pattern;
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
    BalancerUtils.verifyClearBrokerValidness(
        config.clusterInfo(),
        balancingMode.demoted()::contains,
        balancingMode.balancing()::contains,
        allowedTopics);

    final var currentClusterInfo =
        BalancerUtils.clearedCluster(
            config.clusterInfo(),
            balancingMode.demoted()::contains,
            balancingMode.balancing()::contains);
    final var clusterBean = config.clusterBean();
    final var allocationTweaker =
        ShuffleTweaker.builder()
            .numberOfShuffle(() -> ThreadLocalRandom.current().nextInt(minStep, maxStep))
            .allowedTopics(allowedTopics)
            .allowedBrokers(balancingMode.balancing()::contains)
            .build();
    final var moveCostFunction = config.moveCostFunction();

    final Function<ClusterInfo, ClusterCost> evaluateCost =
        (cluster) -> {
          final var filteredCluster =
              !balancingMode.demoted().isEmpty()
                  ? ClusterInfo.builder(cluster)
                      .removeNodes(balancingMode.demoted()::contains)
                      .build()
                  : cluster;
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
                    config.clusterInfo(),
                    currentCost,
                    newAllocation,
                    evaluateCost.apply(newAllocation)))
        .filter(plan -> plan.proposalClusterCost().value() < currentCost.value())
        .min(Comparator.comparing(plan -> plan.proposalClusterCost().value()));
  }
}
