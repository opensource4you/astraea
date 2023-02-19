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
package org.astraea.common.balancer;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.metrics.collector.MetricCollector;
import org.astraea.common.metrics.collector.MetricSensor;

public class BalancerConsoleImpl implements BalancerConsole {

  private final Admin admin;
  private final Function<Integer, Optional<Integer>> jmxPortMapper;

  private final Map<String, CompletionStage<Balancer.Plan>> planGenerations =
      new ConcurrentHashMap<>();
  private final Map<String, CompletionStage<Void>> planExecutions = new ConcurrentHashMap<>();
  private final AtomicReference<String> lastExecutingTask = new AtomicReference<>();

  private final AtomicBoolean closed = new AtomicBoolean(false);

  public BalancerConsoleImpl(Admin admin, Function<Integer, Optional<Integer>> jmxPortMapper) {
    this.admin = admin;
    this.jmxPortMapper = jmxPortMapper;
  }

  @Override
  public Set<String> tasks() {
    return Collections.unmodifiableSet(planGenerations.keySet());
  }

  @Override
  public Optional<TaskPhase> taskPhase(String taskId) {
    var plan = planGenerations.get(taskId);
    if (plan == null) return Optional.empty();
    if (plan.toCompletableFuture().isCompletedExceptionally()
        || plan.toCompletableFuture().isCancelled()) return Optional.of(TaskPhase.SearchFailed);
    if (!plan.toCompletableFuture().isDone()) return Optional.of(TaskPhase.Searching);
    var execution = planExecutions.get(taskId);
    if (execution == null) return Optional.of(TaskPhase.Searched);
    if (execution.toCompletableFuture().isCompletedExceptionally()
        || execution.toCompletableFuture().isCancelled())
      return Optional.of(TaskPhase.ExecutionFailed);
    if (!execution.toCompletableFuture().isDone()) return Optional.of(TaskPhase.Executing);
    return Optional.of(TaskPhase.Executed);
  }

  @Override
  public Generation launchRebalancePlanGeneration() {
    return new Generation() {
      private String taskId;
      private Balancer balancer;
      private AlgorithmConfig algorithmConfig;
      private Duration timeout = Duration.ofSeconds(1);
      private boolean checkNoOngoingMigration = true;

      @Override
      public Generation setTaskId(String taskId) {
        this.taskId = taskId;
        return this;
      }

      @Override
      public Generation setBalancer(Balancer balancer) {
        this.balancer = balancer;
        return this;
      }

      @Override
      public Generation setGenerationTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
      }

      @Override
      public Generation setAlgorithmConfig(AlgorithmConfig config) {
        this.algorithmConfig = config;
        return this;
      }

      @Override
      public Generation checkNoOngoingMigration(boolean enable) {
        this.checkNoOngoingMigration = enable;
        return this;
      }

      @Override
      public CompletionStage<Balancer.Plan> generate() {
        checkNotClosed();
        var taskId = Objects.requireNonNull(this.taskId);
        var balancer = Objects.requireNonNull(this.balancer);
        var timeout = Objects.requireNonNull(this.timeout);
        var config = Objects.requireNonNull(this.algorithmConfig);
        var sensors =
            Stream.concat(
                    config.clusterCostFunction().metricSensor().stream(),
                    config.moveCostFunction().metricSensor().stream())
                .collect(Collectors.toUnmodifiableList());
        var clusterInfo =
            this.checkNoOngoingMigration
                ? BalancerConsoleImpl.this.checkNoOngoingMigration()
                : admin.topicNames(false).thenCompose(admin::clusterInfo);
        return planGenerations.compute(
            taskId,
            (id, previousTask) -> {
              if (previousTask != null)
                throw new IllegalStateException("Conflict task ID: " + taskId);
              return clusterInfo
                  .thenApply(
                      cluster ->
                          metricContext(
                              sensors,
                              (clusterBeanSupplier -> {
                                // TODO: embedded the retry offer logic into BalancerConsoleImpl
                                return balancer.retryOffer(
                                    cluster,
                                    timeout,
                                    AlgorithmConfig.builder(config)
                                        .metricSource(clusterBeanSupplier)
                                        .build());
                              })))
                  .thenApply(
                      plan -> {
                        if (plan.solution().isPresent()) return plan;
                        else
                          throw new IllegalStateException(
                              "Unable to find a rebalance plan that can improve this cluster");
                      });
            });
      }
    };
  }

  @Override
  public Execution launchRebalancePlanExecution() {
    return new Execution() {
      private RebalancePlanExecutor executor;
      private Duration timeout;
      private boolean checkPlanConsistency = true;
      private boolean checkNoOngoingMigration = true;

      @Override
      public Execution setExecutor(RebalancePlanExecutor executor) {
        this.executor = executor;
        return this;
      }

      @Override
      public Execution setExecutionTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
      }

      @Override
      public Execution checkPlanConsistency(boolean enable) {
        this.checkPlanConsistency = enable;
        return this;
      }

      @Override
      public Execution checkNoOngoingMigration(boolean enable) {
        this.checkNoOngoingMigration = enable;
        return this;
      }

      @Override
      public CompletionStage<Void> execute(String taskId) {
        checkNotClosed();
        var planGen = planGenerations.get(taskId);
        if (planGen == null) throw new IllegalArgumentException("No such balance task: " + taskId);

        return planExecutions.computeIfAbsent(
            taskId,
            (id) -> {
              synchronized (this) {
                // another task is still running
                if (lastExecutingTask.get() != null
                    && taskPhase(lastExecutingTask.get()).orElseThrow() == TaskPhase.Executing)
                  throw new IllegalStateException(
                      "Another task is executing: " + lastExecutingTask.get());
                lastExecutingTask.set(taskId);
              }

              return planGen
                  .thenCompose(
                      plan ->
                          checkPlanConsistency
                              ? BalancerConsoleImpl.this.checkPlanConsistency(plan)
                              : CompletableFuture.completedStage(null))
                  .thenCompose(
                      ignore ->
                          checkNoOngoingMigration
                              ? BalancerConsoleImpl.this.checkNoOngoingMigration()
                              : CompletableFuture.completedStage(null))
                  .thenCompose(ignore -> planGen)
                  .thenCompose(
                      plan ->
                          plan.solution()
                              .map(Balancer.Solution::proposal)
                              .map(target -> executor.run(admin, target, timeout))
                              .orElseThrow(
                                  () ->
                                      new IllegalStateException(
                                          "Plan generation failed to find any usable balance plan that will improve this cluster: "
                                              + taskId)));
            });
      }
    };
  }

  @Override
  public void close() {
    if (closed.get()) return;
    synchronized (this) {
      // reject further request
      closed.set(true);
    }
  }

  private void checkNotClosed() {
    if (closed.get()) throw new IllegalStateException("This BalanceConsole is already stopped");
  }

  private CompletionStage<ClusterInfo> checkNoOngoingMigration() {
    return admin
        .topicNames(false)
        .thenCompose(admin::clusterInfo)
        .thenApply(
            cluster -> {
              var ongoingMigration =
                  cluster.replicas().stream()
                      .filter(r -> r.isAdding() || r.isRemoving() || r.isFuture())
                      .map(Replica::topicPartitionReplica)
                      .collect(Collectors.toUnmodifiableSet());
              if (!ongoingMigration.isEmpty())
                throw new IllegalStateException(
                    "Another rebalance task might be working on. "
                        + "The following topic/partition has ongoing migration: "
                        + ongoingMigration);
              return cluster;
            });
  }

  private CompletionStage<Void> checkPlanConsistency(Balancer.Plan plan) {
    final var before =
        plan
            .initialClusterInfo
            .replicaStream()
            .collect(Collectors.groupingBy(Replica::topicPartition))
            .entrySet()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Map.Entry::getKey,
                    e ->
                        e.getValue().stream()
                            // have to compare by isLeader instead of isPreferredLeader. since the
                            // leadership is what affects the direction of traffic load. Any
                            // bandwidth related cost function should calculate load by leadership
                            // instead of preferred leadership.
                            .sorted(
                                Comparator.comparing(Replica::isPreferredLeader)
                                    .reversed()
                                    .thenComparing(x -> x.nodeInfo().id()))
                            .map(x -> Map.entry(x.nodeInfo().id(), x.path()))
                            .collect(Collectors.toUnmodifiableList())));
    return admin
        .topicNames(false)
        .thenCompose(admin::clusterInfo)
        .thenAccept(
            currentCluster -> {
              var now =
                  currentCluster
                      .replicaStream()
                      .collect(Collectors.groupingBy(Replica::topicPartition))
                      .entrySet()
                      .stream()
                      .collect(
                          Collectors.toUnmodifiableMap(
                              Map.Entry::getKey,
                              e ->
                                  e.getValue().stream()
                                      .sorted(
                                          Comparator.comparing(Replica::isPreferredLeader)
                                              .reversed()
                                              .thenComparing(x -> x.nodeInfo().id()))
                                      .map(x -> Map.entry(x.nodeInfo().id(), x.path()))
                                      .collect(Collectors.toUnmodifiableList())));
              var mismatchPartitions =
                  before.entrySet().stream()
                      .filter(
                          e -> {
                            final var tp = e.getKey();
                            final var beforeList = e.getValue();
                            final var nowList = now.get(tp);
                            return !Objects.equals(beforeList, nowList);
                          })
                      .map(Map.Entry::getKey)
                      .collect(Collectors.toUnmodifiableSet());
              if (!mismatchPartitions.isEmpty())
                throw new IllegalStateException(
                    "The cluster state has been changed significantly. "
                        + "The following topic/partitions have different replica list(lookup the moment of plan generation): "
                        + mismatchPartitions);
            });
  }

  private Balancer.Plan metricContext(
      Collection<MetricSensor> metricSensors,
      Function<Supplier<ClusterBean>, Balancer.Plan> execution) {
    // TODO: use a global metric collector when we are ready to enable long-run metric sampling
    //  https://github.com/skiptests/astraea/pull/955#discussion_r1026491162
    try (var collector = MetricCollector.builder().interval(Duration.ofSeconds(1)).build()) {
      freshJmxAddresses().forEach(collector::registerJmx);
      metricSensors.forEach(collector::addMetricSensor);
      return execution.apply(collector::clusterBean);
    }
  }

  // visible for test
  Map<Integer, InetSocketAddress> freshJmxAddresses() {
    var brokers = admin.brokers().toCompletableFuture().join();
    var jmxAddresses =
        brokers.stream()
            .map(broker -> Map.entry(broker, jmxPortMapper.apply(broker.id())))
            .filter(entry -> entry.getValue().isPresent())
            .collect(
                Collectors.toUnmodifiableMap(
                    e -> e.getKey().id(),
                    e ->
                        InetSocketAddress.createUnresolved(
                            e.getKey().host(), e.getValue().orElseThrow())));

    // JMX is disabled
    if (jmxAddresses.size() == 0) return Map.of();

    // JMX is partially enabled, forbidden this use case since it is probably a bad idea
    if (brokers.size() != jmxAddresses.size())
      throw new IllegalArgumentException(
          "Some brokers has no JMX port specified in the web service argument: "
              + brokers.stream()
                  .map(NodeInfo::id)
                  .filter(id -> !jmxAddresses.containsKey(id))
                  .collect(Collectors.toUnmodifiableSet()));

    return jmxAddresses;
  }
}
