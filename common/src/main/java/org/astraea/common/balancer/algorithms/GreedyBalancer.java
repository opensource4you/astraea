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
import java.util.stream.Collectors;
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

  public static final String SHUFFLE_PLAN_GENERATOR_MIN_STEP_CONFIG =
      "shuffle.plan.generator.min.step";
  public static final String SHUFFLE_PLAN_GENERATOR_MAX_STEP_CONFIG =
      "shuffle.plan.generator.max.step";
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
            .string(SHUFFLE_PLAN_GENERATOR_MIN_STEP_CONFIG)
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(1);
    maxStep =
        config
            .config()
            .string(SHUFFLE_PLAN_GENERATOR_MAX_STEP_CONFIG)
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(30);
    iteration =
        config
            .config()
            .string(ITERATION_CONFIG)
            .map(Integer::parseInt)
            .map(Utils::requirePositive)
            .orElse(Integer.MAX_VALUE);
  }

  @Override
  public Optional<Plan> offer(ClusterInfo<Replica> currentClusterInfo, Duration timeout) {
    final var allocationTweaker = new ShuffleTweaker(minStep, maxStep);
    final var metrics = config.metricSource().get();
    final var clusterCostFunction = config.clusterCostFunction();
    final var moveCostFunction = config.moveCostFunctions();
    final var initialCost = clusterCostFunction.clusterCost(currentClusterInfo, metrics);

    final var loop = new AtomicInteger(iteration);
    final var start = System.currentTimeMillis();
    final var executionTime = timeout.toMillis();
    Supplier<Boolean> moreRoom =
        () -> System.currentTimeMillis() - start < executionTime && loop.getAndDecrement() > 0;
    BiFunction<ClusterInfo<Replica>, ClusterCost, Optional<Balancer.Plan>> next =
        (currentAllocation, currentCost) ->
            allocationTweaker
                .generate(currentAllocation)
                .takeWhile(ignored -> moreRoom.get())
                .map(
                    newAllocation -> {
                      var newClusterInfo =
                          ClusterInfo.update(currentClusterInfo, newAllocation::replicas);
                      return new Balancer.Plan(
                          newAllocation,
                          initialCost,
                          clusterCostFunction.clusterCost(newClusterInfo, metrics),
                          moveCostFunction.stream()
                              .map(cf -> cf.moveCost(currentClusterInfo, newClusterInfo, metrics))
                              .collect(Collectors.toList()));
                    })
                .filter(
                    plan ->
                        config.clusterConstraint().test(currentCost, plan.proposalClusterCost()))
                .filter(plan -> config.movementConstraint().test(plan.moveCost()))
                .findFirst();
    var currentCost = initialCost;
    var currentAllocation = ClusterInfo.masked(currentClusterInfo, config.topicFilter());
    var currentPlan = Optional.<Balancer.Plan>empty();

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
      currentPlan = newPlan;
      currentCost = currentPlan.get().proposalClusterCost();
      currentAllocation = currentPlan.get().proposal();
    }
    return currentPlan;
  }
}
