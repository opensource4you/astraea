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

import java.time.Duration;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.cost.NoSufficientMetricsException;
import org.astraea.common.metrics.ClusterBean;

public class BalancerConsoleImpl implements BalancerConsole {

  private final Admin admin;

  private final Map<String, CompletionStage<Balancer.Plan>> planGenerations =
      new ConcurrentHashMap<>();
  private final Map<String, CompletionStage<Void>> planExecutions = new ConcurrentHashMap<>();
  private final AtomicReference<String> lastExecutingTask = new AtomicReference<>();

  private final AtomicBoolean closed = new AtomicBoolean(false);

  public BalancerConsoleImpl(Admin admin) {
    this.admin = admin;
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
      private boolean checkNoOngoingMigration = true;
      private Supplier<ClusterBean> clusterBeanSource = () -> ClusterBean.EMPTY;

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
      public Generation setAlgorithmConfig(AlgorithmConfig config) {
        this.algorithmConfig = config;
        return this;
      }

      @Override
      public Generation setClusterBeanSource(Supplier<ClusterBean> clusterBeanSource) {
        this.clusterBeanSource = clusterBeanSource;
        return this;
      }

      @Override
      public Generation checkNoOngoingMigration(boolean enable) {
        this.checkNoOngoingMigration = enable;
        return this;
      }

      @Override
      public CompletionStage<Balancer.Plan> generate() {
        synchronized (BalancerConsoleImpl.this) {
          checkNotClosed();

          var taskId = Objects.requireNonNull(this.taskId);
          var balancer = Objects.requireNonNull(this.balancer);
          var config = Objects.requireNonNull(this.algorithmConfig);
          var metricSource = Objects.requireNonNull(this.clusterBeanSource);
          var clusterInfo =
              this.checkNoOngoingMigration
                  ? BalancerConsoleImpl.this.checkNoOngoingMigration()
                  : admin.topicNames(false).thenCompose(admin::clusterInfo);
          return planGenerations.compute(
              taskId,
              (id, previousTask) -> {
                if (previousTask != null)
                  throw new IllegalStateException("Conflict task ID: " + taskId);
                return clusterInfo.thenApply(
                    cluster ->
                        retryOffer(
                            balancer,
                            metricSource,
                            AlgorithmConfig.builder(config).clusterInfo(cluster).build()));
              });
        }
      }
    };
  }

  @Override
  public Execution launchRebalancePlanExecution() {
    return new Execution() {
      private RebalancePlanExecutor executor;
      private Duration timeout = Duration.ofHours(3);
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
        var executor = Objects.requireNonNull(this.executor);
        var timeout = Objects.requireNonNull(this.timeout);
        var planGen = planGenerations.get(taskId);
        if (planGen == null) throw new IllegalArgumentException("No such balance task: " + taskId);

        return planExecutions.computeIfAbsent(
            taskId,
            (id) -> {
              synchronized (BalancerConsoleImpl.this) {
                checkNotClosed();

                // another task is still running
                if (lastExecutingTask.get() != null
                    && taskPhase(lastExecutingTask.get()).orElseThrow() == TaskPhase.Executing)
                  throw new IllegalStateException(
                      "Another task is executing: " + lastExecutingTask.get());
                lastExecutingTask.set(taskId);

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
                    .thenCompose(plan -> executor.run(admin, plan.proposal(), timeout));
              }
            });
      }
    };
  }

  @Override
  public void close() {
    if (closed.get()) return;

    synchronized (BalancerConsoleImpl.this) {
      closed.set(true);
    }

    planGenerations.values().forEach(i -> i.toCompletableFuture().cancel(true));
    planExecutions.values().forEach(i -> i.toCompletableFuture().cancel(true));
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
            .initialClusterInfo()
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
                                    .thenComparing(x -> x.broker().id()))
                            .map(x -> Map.entry(x.broker().id(), x.path()))
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
                                              .thenComparing(x -> x.broker().id()))
                                      .map(x -> Map.entry(x.broker().id(), x.path()))
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
                        + "The following topic/partitions have different replica list: "
                        + mismatchPartitions);
            });
  }

  static Balancer.Plan retryOffer(
      Balancer balancer, Supplier<ClusterBean> clusterBeanSupplier, AlgorithmConfig config) {
    final var timeoutMs = System.currentTimeMillis() + config.timeout().toMillis();
    while (System.currentTimeMillis() < timeoutMs) {
      try {
        var plan =
            balancer.offer(
                AlgorithmConfig.builder(config)
                    .clusterBean(clusterBeanSupplier.get())
                    .timeout(Duration.ofMillis(timeoutMs - System.currentTimeMillis()))
                    .build());
        if (plan.isPresent()) return plan.get();
      } catch (NoSufficientMetricsException e) {
        e.printStackTrace();
        var remainTimeout = timeoutMs - System.currentTimeMillis();
        var waitMs = e.suggestedWait().toMillis();
        if (remainTimeout > waitMs) {
          Utils.sleep(Duration.ofMillis(waitMs));
        } else {
          // This suggested wait time will definitely time out after we woke up
          throw new RuntimeException(
              "Execution time will exceeded, "
                  + "remain: "
                  + remainTimeout
                  + "ms, suggestedWait: "
                  + waitMs
                  + "ms.",
              e);
        }
      }
    }
    throw new RuntimeException("Execution time exceeded: " + timeoutMs);
  }
}
