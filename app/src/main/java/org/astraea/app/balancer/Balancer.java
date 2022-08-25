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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.balancer.executor.RebalanceAdmin;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.metrics.IdentifiedFetcher;
import org.astraea.app.balancer.metrics.MetricSource;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.cost.HasClusterCost;
import org.astraea.app.cost.HasMoveCost;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.partitioner.Configuration;

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
            .map(cf -> Utils.constructCostFunction(cf, configuration))
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
            balancerConfigs.metricSourceClass, configuration, fetcherOwnership.values());
    this.topicFilter = (topicName) -> !balancerConfigs.ignoredTopics.contains(topicName);
    this.isClosed = new AtomicBoolean(false);
  }

  /** Run balancer */
  public void run() {
    // run
    while (!Thread.currentThread().isInterrupted()) {
      boolean shouldDrainMetrics = false;
      // let metric warm up
      // TODO: find a way to show the progress, without pollute the logic
      System.out.println("Warmup metrics");
      var t = BalancerUtils.progressWatch("Warm Up Metrics", 1, metricSource::warmUpProgress);
      t.start();
      metricSource.awaitMetricReady();
      metricSource.allBeans().entrySet().stream()
          .collect(
              Collectors.toMap(
                  x -> x.getKey(),
                  x -> x.getValue().values().stream().mapToInt(a -> a.size()).max()))
          .forEach(
              (g, d) -> {
                System.out.println(d.orElse(0));
              });
      // TODO: find a way to show the progress, without pollute the logic
      t.interrupt();
      Utils.packException(() -> t.join());
      System.out.println("Metrics warmed");

      // ensure not working
      var topics =
          admin.topicNames().stream().filter(topicFilter).collect(Collectors.toUnmodifiableSet());
      var migrationInProgress =
          admin.replicas(topics).entrySet().stream()
              .flatMap(x -> x.getValue().stream().map(y -> Map.entry(x.getKey(), y)))
              .filter(r -> !r.getValue().inSync() || r.getValue().isFuture())
              .map(
                  r ->
                      TopicPartitionReplica.of(
                          r.getKey().topic(), r.getKey().partition(), r.getValue().broker()))
              .collect(Collectors.toUnmodifiableList());
      if (!migrationInProgress.isEmpty()) {
        throw new IllegalStateException(
            "There are some migration in progress... " + migrationInProgress);
      }

      try {
        // calculate the score of current cluster
        var clusterInfo = newClusterInfo();
        var clusterMetrics = metricSource.allBeans();
        var currentClusterScore = evaluateCost(clusterInfo, clusterMetrics);
        // TODO: find a way to show the progress, without pollute the logic
        System.out.println("Run " + planGenerator.getClass().getName());
        if (currentClusterScore >= 1.0) continue;
        var bestProposal = seekingRebalancePlan(currentClusterScore, clusterInfo, clusterMetrics);
        // TODO: find a way to show the progress, without pollute the logic
        System.out.println(bestProposal);
        var bestCluster = BalancerUtils.merge(clusterInfo, bestProposal.rebalancePlan());
        var bestScore = evaluateCost(bestCluster, clusterMetrics);
        System.out.printf(
            "Current cluster score: %.8f, Proposed cluster score: %.8f%n",
            currentClusterScore, bestScore);
        if (!isPlanExecutionWorth(clusterInfo, bestProposal, currentClusterScore, bestScore)) {
          // TODO: find a way to show the progress, without pollute the logic
          System.out.println("The proposed plan is rejected due to no worth improvement");
          continue;
        }
        // TODO: find a way to show the progress, without pollute the logic
        System.out.println("Run " + planExecutor.getClass().getName());
        shouldDrainMetrics = true;
        executePlan(bestProposal);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        // drain old metrics, these metrics is probably invalid after the rebalance operation
        // performed.
        if (shouldDrainMetrics) metricSource.drainMetrics();
      }
    }
  }

  /** Retrieve a new {@link ClusterInfo}, with info only related to the permitted topics. */
  private ClusterInfo newClusterInfo() {
    var topics =
        admin.topicNames().stream().filter(topicFilter).collect(Collectors.toUnmodifiableSet());
    return admin.clusterInfo(topics);
  }

  private RebalancePlanProposal seekingRebalancePlan(
      double currentScore,
      ClusterInfo clusterInfo,
      Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> clusterMetrics) {
    var tries = balancerConfigs.planSearchingIteration;
    var counter = new LongAdder();
    // TODO: find a way to show the progress, without pollute the logic
    var thread = BalancerUtils.progressWatch("Searching for Good Rebalance Plan", tries, counter::doubleValue);
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

      // Find the plan with smallest move cost
      var bestMigrationProposal =
          bestMigrationProposals.stream()
              .min(
                  Comparator.comparing(
                      entry -> {
                        var proposal = entry.getValue();
                        var allocation = proposal.rebalancePlan();
                        var mockedCluster = BalancerUtils.merge(clusterInfo, allocation);
                        return evaluateMoveCost(mockedCluster, clusterMetrics);
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
    // prepare context
    var allocation = proposal.rebalancePlan();
    try (Admin newAdmin = Admin.of(balancerConfigs.bootstrapServers())) {
      var rebalanceAdmin = RebalanceAdmin.of(newAdmin, topicFilter);

      // execute
      planExecutor.run(rebalanceAdmin, allocation);
    }
  }

  // visible for testing
  boolean isPlanExecutionWorth(
      ClusterInfo currentCluster,
      RebalancePlanProposal proposal,
      double currentScore,
      double proposedScore) {
    return currentScore > proposedScore;
  }

  private double evaluateCost(
      ClusterInfo clusterInfo,
      Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> metrics) {
    var scores =
        costFunctions.stream()
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
      ClusterInfo clusterInfo,
      Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> metrics) {
    var scores =
        costFunctions.stream()
            .map(
                cf -> {
                  var fetcher = fetcherOwnership.get(cf);
                  var theMetrics = metrics.get(fetcher);
                  var clusterBean = ClusterBean.of(theMetrics);
                  return Map.entry(cf, this.moveCostScore(clusterInfo, clusterBean, cf));
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return aggregateFunction(scores);
  }

  /** the lower, the better. */
  private double aggregateFunction(Map<? extends CostFunction, Double> scores) {
    scores.forEach(
        (func, value) -> {
          // System.out.printf("[%s] %.8f%n", func.getClass().getSimpleName(), value);
        });
    // use the simple summation result, treat every cost equally.
    return scores.values().stream().mapToDouble(x -> x).sum();
  }

  private double costFunctionScore(
      ClusterInfo clusterInfo, ClusterBean clusterBean, CostFunction costFunction) {
    if (costFunction instanceof HasClusterCost) {
      return ((HasClusterCost) costFunction).clusterCost(clusterInfo, clusterBean).value();
    } else if (costFunction instanceof HasBrokerCost) {
      return brokerCostScore(clusterInfo, (HasBrokerCost) costFunction);
    } else {
      return 0.0;
      /* throw new IllegalArgumentException(
         "Unable to extract score from this cost function: " + costFunction.getClass().getName());
      */
    }
  }

  private double moveCostScore(
      ClusterInfo clusterInfo, ClusterBean clusterBean, CostFunction costFunction) {
    if (costFunction instanceof HasMoveCost) {
      var originalClusterInfo = newClusterInfo();
      var targetAllocation = ClusterLogAllocation.of(clusterInfo);
      if (((HasMoveCost) costFunction).overflow(originalClusterInfo, clusterInfo, clusterBean))
        return 999999.0;
      return ((HasMoveCost) costFunction)
          .clusterCost(originalClusterInfo, clusterInfo, clusterBean)
          .value();
    }
    return 0.0;
  }

  private <T extends HasBrokerCost> double brokerCostScore(
      ClusterInfo clusterInfo, T costFunction) {
    // TODO: revise the default usage
    var metrics = metricSource.allBeans().get(fetcherOwnership.get(costFunction));
    var clusterBean = ClusterBean.of(metrics);
    return costFunction.brokerCost(clusterInfo, clusterBean).value().values().stream()
        .mapToDouble(x -> x)
        .max()
        .orElseThrow();
  }

  // TODO: this usage will be removed someday
  @Deprecated
  public static Thread progressWatch(String title, double totalTasks, Supplier<Double> accTasks) {
    AtomicInteger counter = new AtomicInteger();

    Supplier<String> nextProgressBar =
        () -> {
          int blockCount = 20;
          double percentagePerBlock = 1.0 / blockCount;
          double now = accTasks.get();
          double currentProgress = now / totalTasks;
          int fulfilled = Math.min((int) (currentProgress / percentagePerBlock), blockCount);
          int rollingBlock = blockCount - fulfilled >= 1 ? 1 : 0;
          int emptyBlocks = blockCount - rollingBlock - fulfilled;

          String rollingText = "-\\|/";
          String filled = String.join("", Collections.nCopies(fulfilled, "-"));
          String rolling =
              String.join(
                  "",
                  Collections.nCopies(
                      rollingBlock, "" + rollingText.charAt(counter.getAndIncrement() % 4)));
          String empty = String.join("", Collections.nCopies(emptyBlocks, " "));
          return String.format("[%s%s%s] (%.2f/%.2f)", filled, rolling, empty, now, totalTasks);
        };

    Runnable progressWatch =
        () -> {
          while (!Thread.currentThread().isInterrupted()) {
            System.out.print("[" + title + "] " + nextProgressBar.get() + '\r');
            try {
              TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
              break;
            }
          }
          System.out.println("[" + title + "] " + nextProgressBar.get() + '\r');
          System.out.println();
        };

    return new Thread(progressWatch);
  }

  @Override
  public void close() {
    // avoid being closed twice
    if (isClosed.getAndSet(true)) return;
    admin.close();
    metricSource.close();
  }
}
