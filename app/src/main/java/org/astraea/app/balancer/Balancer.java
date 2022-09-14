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
package org.astraea.app.balancer;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.astraea.app.balancer.executor.RebalanceAdmin;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.metrics.IdentifiedFetcher;
import org.astraea.app.balancer.metrics.MetricSource;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.cost.Configuration;
import org.astraea.common.cost.CostFunction;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.metrics.HasBeanObject;

public class Balancer implements AutoCloseable {

  private final Admin admin;
  private final RebalancePlanGenerator planGenerator;
  private final RebalancePlanExecutor planExecutor;
  private final MetricSource metricSource;
  private final List<? extends CostFunction> costFunctions;
  private final BalancerConfigs balancerConfigs;
  private final Configuration configuration;
  private final Predicate<String> topicFilter;
  private final Map<CostFunction, IdentifiedFetcher> fetcherOwnership;
  private final AtomicBoolean isClosed;

  public Balancer(BalancerConfigs balancerConfigs) {
    this.balancerConfigs = balancerConfigs;
    this.configuration = Configuration.of(balancerConfigs.configs());
    this.admin = Admin.of(balancerConfigs.configs());
    this.planGenerator =
        BalancerUtils.constructGenerator(
            balancerConfigs.rebalancePlanGeneratorClass, configuration);
    this.planExecutor =
        BalancerUtils.constructExecutor(balancerConfigs.rebalancePlanExecutorClass, configuration);
    this.costFunctions =
        balancerConfigs.costFunctionClasses.stream()
            .map(cf -> Utils.construct(cf, configuration))
            .collect(Collectors.toUnmodifiableList());
    this.fetcherOwnership =
        costFunctions.stream()
            .map(
                costFunction ->
                    costFunction
                        .fetcher()
                        .map(fetcher -> Map.entry(costFunction, new IdentifiedFetcher(fetcher))))
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    this.metricSource =
        BalancerUtils.constructMetricSource(
            balancerConfigs.metricSourceClass,
            balancerConfigs.jmxServers,
            fetcherOwnership.values(),
            configuration);
    this.topicFilter = (topicName) -> !balancerConfigs.ignoredTopics.contains(topicName);
    this.isClosed = new AtomicBoolean(false);
  }

  /** Run balancer */
  public void run() {
    // run
    while (!Thread.currentThread().isInterrupted()) {
      boolean shouldDrainMetrics = false;
      System.out.println("[Fetch metrics]");
      awaitMetrics();

      System.out.println("[Ensure no on going migration]");
      ensureNoOnGoingMigration();

      try {
        // calculate the score of current cluster
        var clusterInfo = newClusterInfo();
        var clusterMetrics = metricSource.allBeans();
        var currentClusterScore = evaluateCost(clusterInfo, clusterMetrics);

        System.out.println("[Run " + planGenerator.getClass().getName() + "]");
        var bestProposal = seekingRebalancePlan(currentClusterScore, clusterInfo, clusterMetrics);
        var bestCluster = BalancerUtils.merge(clusterInfo, bestProposal.rebalancePlan());
        var bestScore = evaluateCost(bestCluster, clusterMetrics);

        System.out.println(bestProposal);
        System.out.printf(
            "Current cluster score: %.8f, Proposed cluster score: %.8f%n",
            currentClusterScore, bestScore);
        if (currentClusterScore > bestScore) {
          System.out.println("[Run " + planExecutor.getClass().getName() + "]");
          shouldDrainMetrics = true;
          executePlan(bestProposal);
        } else {
          System.out.println("The proposed plan is rejected due to no worth improvement");
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        // drain old metrics, these metrics are probably invalid after the rebalance operation.
        if (shouldDrainMetrics) metricSource.drainMetrics();
      }
    }
  }

  private void ensureNoOnGoingMigration() {
    var topics =
        admin.topicNames().stream().filter(topicFilter).collect(Collectors.toUnmodifiableSet());
    var migrationInProgress =
        admin.replicas(topics).entrySet().stream()
            .flatMap(x -> x.getValue().stream().map(y -> Map.entry(x.getKey(), y)))
            .filter(r -> !r.getValue().inSync() || r.getValue().isFuture())
            .map(
                r ->
                    TopicPartitionReplica.of(
                        r.getKey().topic(), r.getKey().partition(), r.getValue().nodeInfo().id()))
            .collect(Collectors.toUnmodifiableList());
    if (!migrationInProgress.isEmpty()) {
      throw new IllegalStateException(
          "There are some migration in progress... " + migrationInProgress);
    }
  }

  private void awaitMetrics() {
    var t = BalancerUtils.progressWatch("Warm Up Metrics", 1, metricSource::warmUpProgress);
    t.start();
    metricSource.awaitMetricReady();
    t.interrupt();
    Utils.packException(() -> t.join());
  }

  /** Retrieve a new {@link ClusterInfo}, with info only related to the permitted topics. */
  private ClusterInfo<Replica> newClusterInfo() {
    var topics =
        admin.topicNames().stream().filter(topicFilter).collect(Collectors.toUnmodifiableSet());
    return admin.clusterInfo(topics);
  }

  private RebalancePlanProposal seekingRebalancePlan(
      double currentScore,
      ClusterInfo<Replica> clusterInfo,
      Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> clusterMetrics) {
    var tries = balancerConfigs.planSearchingIteration;
    var counter = new LongAdder();
    var thread =
        BalancerUtils.progressWatch(
            "Searching for Good Rebalance Plan", tries, counter::doubleValue);
    try {
      thread.start();
      var bestMigrationProposals =
          planGenerator
              .generate(admin.brokerFolders(), ClusterLogAllocation.of(clusterInfo))
              .parallel()
              .limit(tries)
              .peek(ignore -> counter.increment())
              .map(
                  plan -> {
                    var allocation = plan.rebalancePlan();
                    var mockedCluster = BalancerUtils.merge(clusterInfo, allocation);
                    var score = evaluateCost(mockedCluster, clusterMetrics);
                    return Map.entry(score, plan);
                  })
              .filter(x -> x.getKey() < currentScore)
              .sorted(Map.Entry.comparingByKey())
              .limit(300)
              .collect(Collectors.toUnmodifiableList());

      // Find the plan with the smallest move cost
      var bestMigrationProposal =
          bestMigrationProposals.stream()
              .min(
                  Comparator.comparing(
                      entry -> {
                        var proposal = entry.getValue();
                        var allocation = proposal.rebalancePlan();
                        var mockedCluster = BalancerUtils.merge(clusterInfo, allocation);
                        return evaluateMoveCost(clusterInfo, mockedCluster, clusterMetrics);
                      }));

      // find the target with the highest score, return it
      return bestMigrationProposal
          .map(Map.Entry::getValue)
          .orElseThrow(() -> new NoSuchElementException("No Better Plan Found"));
    } finally {
      thread.interrupt();
      Utils.packException(() -> thread.join());
    }
  }

  private void executePlan(RebalancePlanProposal proposal) {
    try (Admin newAdmin = Admin.of(balancerConfigs.configs())) {
      planExecutor.run(RebalanceAdmin.of(newAdmin, topicFilter), proposal.rebalancePlan());
    }
  }

  private double evaluateCost(
      ClusterInfo<Replica> clusterInfo,
      Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> metrics) {
    var scores =
        costFunctions.stream()
            .filter(x -> x instanceof HasClusterCost)
            .map(x -> (HasClusterCost) x)
            .map(
                cf -> {
                  var fetcher = fetcherOwnership.get(cf);
                  var theMetrics = metrics.get(fetcher);
                  var clusterBean = ClusterBean.of(theMetrics);
                  return Map.entry(cf, this.costFunctionScore(clusterInfo, clusterBean, cf));
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return aggregateFunction(scores);
  }

  private double evaluateMoveCost(
      ClusterInfo<? extends Replica> currentCluster,
      ClusterInfo<? extends Replica> clusterInfo,
      Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> metrics) {
    var scores =
        costFunctions.stream()
            .filter(cf -> cf instanceof HasMoveCost)
            .map(cf -> (HasMoveCost) cf)
            .map(
                cf -> {
                  var fetcher = fetcherOwnership.get(cf);
                  var theMetrics = metrics.get(fetcher);
                  var clusterBean = ClusterBean.of(theMetrics);
                  return Map.entry(
                      cf, this.moveCostScore(currentCluster, clusterInfo, clusterBean, cf));
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return aggregateFunction(scores);
  }

  private double aggregateFunction(Map<? extends CostFunction, Double> scores) {
    // use the simple summation result, treat every cost equally.
    return scores.values().stream().mapToDouble(x -> x).sum();
  }

  private <T extends HasClusterCost> double costFunctionScore(
      ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean, T costFunction) {
    return costFunction.clusterCost(clusterInfo, clusterBean).value();
  }

  private <T extends HasMoveCost> double moveCostScore(
      ClusterInfo<? extends ReplicaInfo> currentCluster,
      ClusterInfo<? extends ReplicaInfo> clusterInfo,
      ClusterBean clusterBean,
      T costFunction) {
    // TODO: add score here
    return 0;
  }

  @Override
  public void close() {
    // avoid being closed twice
    if (isClosed.getAndSet(true)) return;
    admin.close();
    metricSource.close();
  }
}
