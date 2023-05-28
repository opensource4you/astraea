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

import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
import org.astraea.common.metrics.MBeanRegister;

/**
 * A single-state hill-climbing algorithm. It discovers rebalance solution by tweaking the cluster
 * state multiple times, select the ideal tweak among the discovery. This process might take
 * multiple iterations, until no nicer tweak found.
 */
public class GreedyBalancer implements Balancer {

  public static final String SHUFFLE_TWEAKER_MIN_STEP_CONFIG = "shuffle.tweaker.min.step";

  /**
   * The maximum alteration to a ClusterInfo in one cluster state exploration trial.
   *
   * <p>GreedyBalancer tries to discover a better way to allocate logs, by altering the given
   * current log allocation. This config controls the maximum number of alteration in one go. The
   * larger the number the bolder move it can be made in one go. But it doesn't mean the larger this
   * config the better. On The Contrary, less move usually helps approach good allocation. There are
   * a few things to consider when tuning this number:
   *
   * <h3>Large max step</h3>
   *
   * <ol>
   *   <li>(Good)Make it possible to jump out of a local minimum.
   *   <li>(Bad)The expected score improvement of each alteration is negative most of the time.
   *       Making more moves usually results in the worst state. All these large steps might be a
   *       waste of calculation
   * </ol>
   *
   * <h3>Small max step</h3>
   *
   * <ol>
   *   <li>(Good)Less step equals to less calculation, this can save some considerable processing
   *       resource for balancer to focus on explore the state space
   *   <li>(Good)You can construct a good large move with many small moves, with more confident.
   *   <li>(Good)The Kafka log allocation optimization problem usually come with very high
   *       dimensions. When the dimension is high, we have more chances to slip to a better state.
   *       So we probably won't be stuck at a pretty bad local minimum.
   *   <li>(Bad)When we really stuck at bad local minimum, we won't be able to jump out of it.
   * </ol>
   *
   * <h3>Other discussion</h3>
   *
   * <ol>
   *   <li>The mindset behind greedy balancer is discovering better allocation by making many steps
   *       of move. Not making large move in one go.
   *   <li>Larger move usually results in a terrible cluster state. Although this depends on the
   *       implementation of the allocation tweaker, the allocation tweaker we are using now making
   *       move blinding in the state space. So it might not propose a good move, especially when
   *       more moves have been made the chance of getting a terrible state is higher. Due to this
   *       reason making more moves might make it hard for GreedyBalancer to discover a good plan.
   *   <li>Making many alterations in one go can helps to escape the local minimum. But in the
   *       allocation problem we are facing right now, it is probably hard to get stuck in a bad
   *       local minimum. (Warning, the following thought is totally based on my heuristic and a few
   *       discussion on the Internet. I didn't prove this formally) The local minimum we see one
   *       book is usually depict in two or three dimension space. With such low number of
   *       dimension, it is hard to escape a local minimum since we have fewer dimension to choice
   *       to. But when the dimension getting higher, there is more chance a state can slip out to
   *       somewhere better since there is more choice to made. So it is hard to get stuck in a bad
   *       local minimum when the dimension is higher. We probably don't need to worry about
   *       escaping local minimum when solving the Kafka log allocation problem. The Kafka partition
   *       allocation problem has a feature, it usually have a very high dimension. By dimension I
   *       mean the thing can be tune, for each partition you can consider it as a dimension. So if
   *       a cluster has 5000 partitions, there are 5000 knobs (dimension) that can be tune.
   * </ol>
   */
  public static final String SHUFFLE_TWEAKER_MAX_STEP_CONFIG = "shuffle.tweaker.max.step";

  public static final String ITERATION_CONFIG = "iteration";
  public static final Set<String> ALL_CONFIGS =
      new TreeSet<>(
          Utils.constants(GreedyBalancer.class, name -> name.endsWith("CONFIG"), String.class));

  private final AtomicInteger run = new AtomicInteger();

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
            // Use 5 as the maximum step in each trial, this number provide a good balance between
            // exploration and processing resource.
            .orElse(5);
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
    final Predicate<Integer> isCleaning =
        id -> balancingMode.get(id) == BalancerUtils.BalancingModes.CLEAR;
    final var clearing =
        balancingMode.values().stream().anyMatch(i -> i == BalancerUtils.BalancingModes.CLEAR);
    BalancerUtils.verifyClearBrokerValidness(config.clusterInfo(), isCleaning);

    final var currentClusterInfo =
        BalancerUtils.clearedCluster(config.clusterInfo(), isCleaning, isBalancing);
    final var clusterBean = config.clusterBean();
    final var fixedReplicas =
        config
            .clusterInfo()
            .replicaStream()
            // if a topic is not allowed to move, it should be fixed.
            // if a topic is not allowed to move, but originally it located on a clearing broker, it
            // is ok to move.
            .filter(tpr -> !allowedTopics.test(tpr.topic()) && !isCleaning.test(tpr.broker().id()))
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
              clearing ? ClusterInfo.builder(cluster).removeNodes(isCleaning).build() : cluster;
          return config.clusterCostFunction().clusterCost(filteredCluster, clusterBean);
        };
    final var initialCost = evaluateCost.apply(currentClusterInfo);

    final var loop = new AtomicInteger(iteration);
    final var start = System.currentTimeMillis();
    final var executionTime = config.timeout().toMillis();
    Supplier<Boolean> moreRoom =
        () -> System.currentTimeMillis() - start < executionTime && loop.getAndDecrement() > 0;
    BiFunction<ClusterInfo, ClusterCost, Optional<Plan>> next =
        (currentAllocation, currentCost) ->
            allocationTweaker
                .generate(currentAllocation)
                .takeWhile(ignored -> moreRoom.get())
                .filter(
                    newAllocation ->
                        !moveCostFunction
                            .moveCost(currentClusterInfo, newAllocation, clusterBean)
                            .overflow())
                .map(
                    newAllocation ->
                        new Plan(
                            config.clusterInfo(),
                            initialCost,
                            newAllocation,
                            evaluateCost.apply(newAllocation)))
                .filter(plan -> plan.proposalClusterCost().value() < currentCost.value())
                .findFirst();
    var currentCost = initialCost;
    var currentAllocation = currentClusterInfo;
    var currentSolution = Optional.<Plan>empty();

    // register JMX
    var currentIteration = new LongAdder();
    var currentMinCost =
        new DoubleAccumulator((l, r) -> Double.isNaN(r) ? l : Math.min(l, r), initialCost.value());
    MBeanRegister.local()
        .domainName("astraea.balancer")
        .property("id", config.executionId())
        .property("algorithm", GreedyBalancer.class.getSimpleName())
        .property("run", Integer.toString(run.getAndIncrement()))
        .attribute("Iteration", Long.class, currentIteration::sum)
        .attribute("MinCost", Double.class, currentMinCost::get)
        .register();

    while (true) {
      currentIteration.add(1);
      currentMinCost.accumulate(currentCost.value());
      var newPlan = next.apply(currentAllocation, currentCost);
      if (newPlan.isEmpty()) break;
      currentSolution = newPlan;
      currentCost = currentSolution.get().proposalClusterCost();
      currentAllocation = currentSolution.get().proposal();
    }
    return currentSolution.or(
        () -> {
          // With clearing, the implementation detail start search from a cleared state. It is
          // possible
          // that the start state is already the ideal answer. In this case, it is directly
          // returned.
          if (clearing
              && initialCost.value() == 0.0
              && !moveCostFunction
                  .moveCost(config.clusterInfo(), currentClusterInfo, clusterBean)
                  .overflow()) {
            return Optional.of(
                new Plan(
                    config.clusterInfo(),
                    config.clusterCostFunction().clusterCost(config.clusterInfo(), clusterBean),
                    currentClusterInfo,
                    initialCost));
          }
          return Optional.empty();
        });
  }
}
