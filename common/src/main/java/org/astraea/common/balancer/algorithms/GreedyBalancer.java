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

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.Balancer;
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
      new TreeSet<>(Utils.constants(GreedyBalancer.class, name -> name.endsWith("CONFIG")));

  private final AlgorithmConfig config;
  private final int minStep;
  private final int maxStep;
  private final int iteration;
  private final AtomicInteger run = new AtomicInteger();

  public GreedyBalancer(AlgorithmConfig algorithmConfig) {
    this.config = algorithmConfig;
    minStep =
        config
            .config()
            .string(SHUFFLE_TWEAKER_MIN_STEP_CONFIG)
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(1);
    maxStep =
        config
            .config()
            .string(SHUFFLE_TWEAKER_MAX_STEP_CONFIG)
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(5);
    iteration =
        config
            .config()
            .string(ITERATION_CONFIG)
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(Integer.MAX_VALUE);
  }

  @Override
  public Plan offer(ClusterInfo<Replica> currentClusterInfo, Duration timeout) {
    final var allocationTweaker = new ShuffleTweaker(minStep, maxStep);
    final var metrics = config.metricSource().get();
    final var clusterCostFunction = config.clusterCostFunction();
    final var moveCostFunction = config.moveCostFunction();
    final var initialCost = clusterCostFunction.clusterCost(currentClusterInfo, metrics);

    final var loop = new AtomicInteger(iteration);
    final var start = System.currentTimeMillis();
    final var executionTime = timeout.toMillis();
    Supplier<Boolean> moreRoom =
        () -> System.currentTimeMillis() - start < executionTime && loop.getAndDecrement() > 0;
    BiFunction<ClusterInfo<Replica>, ClusterCost, Optional<Solution>> next =
        (currentAllocation, currentCost) ->
            allocationTweaker
                .generate(currentAllocation)
                .takeWhile(ignored -> moreRoom.get())
                .map(
                    newAllocation -> {
                      var newClusterInfo =
                          ClusterInfo.update(currentClusterInfo, newAllocation::replicas);
                      return new Solution(
                          clusterCostFunction.clusterCost(newClusterInfo, metrics),
                          moveCostFunction.moveCost(currentClusterInfo, newClusterInfo, metrics),
                          newAllocation);
                    })
                .filter(
                    plan ->
                        config.clusterConstraint().test(currentCost, plan.proposalClusterCost()))
                .filter(plan -> config.movementConstraint().test(plan.moveCost()))
                .findFirst();
    var currentCost = initialCost;
    var currentAllocation = ClusterInfo.masked(currentClusterInfo, config.topicFilter());
    var currentSolution = Optional.<Solution>empty();

    // register JMX
    var currentIteration = new LongAdder();
    var currentMinCost =
        new DoubleAccumulator((l, r) -> Double.isNaN(r) ? l : Math.min(l, r), initialCost.value());
    MBeanRegister.local()
        .setDomainName("astraea.balancer")
        .addProperty("id", config.executionId())
        .addProperty("algorithm", GreedyBalancer.class.getSimpleName())
        .addProperty("run", Integer.toString(run.getAndIncrement()))
        .addAttribute("Iteration", Long.class, currentIteration::sum)
        .addAttribute("MinCost", Double.class, currentMinCost::get)
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
    return new Plan(initialCost, currentSolution.orElse(null));
  }
}
