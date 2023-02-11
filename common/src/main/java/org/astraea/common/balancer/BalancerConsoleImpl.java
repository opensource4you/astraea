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
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Utils;
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
  private final Map<String, BalanceTaskImpl> tasks = new ConcurrentHashMap<>();
  private final AtomicReference<BalanceTaskImpl> lastExecutingTask = new AtomicReference<>();
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  public BalancerConsoleImpl(Admin admin, Function<Integer, Optional<Integer>> jmxPortMapper) {
    this.admin = admin;
    this.jmxPortMapper = jmxPortMapper;
  }

  @Override
  public Collection<BalanceTask> tasks() {
    return Collections.unmodifiableCollection(tasks.values());
  }

  @Override
  public Optional<BalanceTask> task(String taskId) {
    return Optional.ofNullable(tasks.get(taskId));
  }

  @Override
  public Generation launchRebalancePlanGeneration() {
    return new Generation() {
      private Balancer balancer;
      private AlgorithmConfig algorithmConfig;
      private Duration timeout = Duration.ofSeconds(1);
      private boolean checkNoOngoingMigration = true;

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
      public BalanceTask generate() {
        final var balancer = Objects.requireNonNull(this.balancer);
        final var timeout = Objects.requireNonNull(this.timeout);
        final var config = Objects.requireNonNull(this.algorithmConfig);
        final var sensors =
            Stream.concat(
                    config.clusterCostFunction().metricSensor().stream(),
                    config.moveCostFunction().metricSensor().stream())
                .collect(Collectors.toUnmodifiableList());
        final var taskId = UUID.randomUUID().toString();
        synchronized (this) {
          checkNotClosed();
          if (this.checkNoOngoingMigration) BalancerConsoleImpl.this.checkNoOngoingMigration();
          if (tasks.containsKey(taskId))
            throw new IllegalStateException("Conflict task ID: " + taskId);
          var sourceCluster =
              admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();
          tasks.put(
              taskId,
              new BalanceTaskImpl(
                  taskId,
                  sourceCluster,
                  CompletableFuture.supplyAsync(
                      () ->
                          metricContext(
                              sensors,
                              (clusterBeanSupplier -> {
                                // TODO: embedded the retry offer logic into BalancerConsoleImpl
                                return balancer.retryOffer(
                                    sourceCluster,
                                    timeout,
                                    AlgorithmConfig.builder(config)
                                        .metricSource(clusterBeanSupplier)
                                        .build());
                              })))));
        }
        return tasks.get(taskId);
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
      public BalanceTask execute(String taskId) {
        if (!tasks.containsKey(taskId))
          throw new IllegalArgumentException("No such balance task: " + taskId);
        var task = tasks.get(taskId);
        if (this.checkNoOngoingMigration) BalancerConsoleImpl.this.checkNoOngoingMigration();
        if (this.checkPlanConsistency) BalancerConsoleImpl.this.checkPlanConsistency(task);
        synchronized (this) {
          checkNotClosed();
          // If this plan already executed, does nothing
          if (task.phase() == BalanceTask.Phase.Executing
              || task.phase() == BalanceTask.Phase.Executed) return task;
          // another task is still running
          if (lastExecutingTask.get() != null
              && lastExecutingTask.get().phase() != BalanceTask.Phase.Executed)
            throw new IllegalStateException(
                "Another task is executing: " + lastExecutingTask.get().id());
          // start the execution. this should fail if the plan is not ready
          task.startExecution(
              (targetClusterInfo) -> executor.run(admin, targetClusterInfo, timeout));
          lastExecutingTask.set(task);
        }
        return task;
      }
    };
  }

  @Override
  public void close() {
    if (stopped.get()) return;
    synchronized (this) {
      // reject further request
      stopped.set(true);

      // stop execution
      if (lastExecutingTask.get() != null) lastExecutingTask.get().planExecution.cancel(false);

      // stop generation
      var taskToStop =
          tasks.values().stream()
              .filter(task -> task.phase() == BalanceTask.Phase.Searching)
              .peek(task -> task.planGeneration.cancel(true))
              .collect(Collectors.toUnmodifiableSet());
      Utils.sleep(Duration.ofSeconds(1));
      taskToStop.stream()
          .filter(task -> !task.planGeneration.isDone())
          .forEach(
              task ->
                  System.err.println(
                      "Failed to stop task plan generation, it still running after interrupt: "
                          + task));

      // wait until execution stop
      try {
        if (lastExecutingTask.get() != null)
          lastExecutingTask.get().planExecution().toCompletableFuture().get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

  private void checkNotClosed() {
    if (stopped.get()) throw new IllegalStateException("This BalanceConsole is already stopped");
  }

  private void checkNoOngoingMigration() {
    var replicas =
        admin
            .topicNames(false)
            .thenCompose(admin::clusterInfo)
            .toCompletableFuture()
            .join()
            .replicas();
    var ongoingMigration =
        replicas.stream()
            .filter(r -> r.isAdding() || r.isRemoving() || r.isFuture())
            .map(Replica::topicPartitionReplica)
            .collect(Collectors.toUnmodifiableSet());
    if (!ongoingMigration.isEmpty())
      throw new IllegalStateException(
          "Another rebalance task might be working on. "
              + "The following topic/partition has ongoing migration: "
              + ongoingMigration);
  }

  private void checkPlanConsistency(BalanceTaskImpl task) {
    final var before =
        task
            .sourceCluster
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
    final var now =
        admin
            .clusterInfo(task.sourceCluster.topics())
            .toCompletableFuture()
            .join()
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

  private static final class BalanceTaskImpl implements BalanceTask {

    private final String taskId;
    private final CompletableFuture<Balancer.Plan> planGeneration;
    private final CompletableFuture<Void> planExecution;
    private final ExecutionLatch latch;
    private final ClusterInfo sourceCluster;

    private BalanceTaskImpl(
        String taskId, ClusterInfo sourceCluster, CompletableFuture<Balancer.Plan> planGeneration) {
      this.taskId = taskId;
      this.latch = new ExecutionLatch();
      this.sourceCluster = sourceCluster;
      this.planGeneration = planGeneration;
      this.planExecution =
          CompletableFuture.runAsync(() -> Utils.packException(this.latch::awaitExecutionStart))
              .thenCompose(ignore -> this.planGeneration)
              .thenCompose(
                  balancePlan ->
                      balancePlan
                          .solution()
                          .map(Balancer.Solution::proposal)
                          .map(
                              cluster ->
                                  Utils.packException(this.latch::awaitExecutionStart)
                                      .apply(cluster))
                          .orElse(
                              CompletableFuture.failedFuture(
                                  new NoSuchElementException(
                                      "Unable to execute this balance plan since no cluster improvement solution found"))));
    }

    @Override
    public String id() {
      return taskId;
    }

    @Override
    public Phase phase() {
      // Note: this method is not transactional
      if (planExecution.isDone() && latch.started()) return Phase.Executed;
      if (planGeneration.isDone() && latch.started()) return Phase.Executing;
      if (planGeneration.isDone() && !latch.started()) return Phase.Searched;
      if (!planGeneration.isDone()) return Phase.Searching;
      throw new IllegalStateException("This should never happened");
    }

    @Override
    public CompletionStage<Balancer.Plan> planGeneration() {
      return planGeneration.minimalCompletionStage();
    }

    @Override
    public CompletionStage<Void> planExecution() {
      return planExecution.minimalCompletionStage();
    }

    /**
     * Launch the execution of this plan.
     *
     * @param executionContext a lambda that return a future that attempts to fulfill the transition
     *     to the given cluster state
     * @throws IllegalStateException if the plan does not exist, has not been generated , or has
     *     already been executed.
     */
    void startExecution(Function<ClusterInfo, CompletionStage<Void>> executionContext) {
      if (!planGeneration.isDone()) throw new IllegalStateException("The plan is not ready");
      if (planGeneration.isCancelled())
        throw new IllegalStateException("The plan generation is cancelled");
      if (planGeneration.isCompletedExceptionally())
        throw new IllegalStateException(
            "The specified plan generation failed with an exception",
            planGeneration.handle((ignore, err) -> err).getNow(null));
      if (planGeneration.getNow(null).solution().isEmpty())
        throw new IllegalStateException(
            "This plan failed to find any usable balance plan that will improve this cluster");
      if (latch.started())
        throw new IllegalStateException("This rebalance execution already started");

      this.latch.startExecution(executionContext);
    }

    private static class ExecutionLatch {

      private final CountDownLatch permitExecution = new CountDownLatch(1);
      private final AtomicReference<Function<ClusterInfo, CompletionStage<Void>>> executionContext =
          new AtomicReference<>();

      public void startExecution(Function<ClusterInfo, CompletionStage<Void>> executionContext) {
        this.executionContext.set(executionContext);
        permitExecution.countDown();
      }

      public Function<ClusterInfo, CompletionStage<Void>> awaitExecutionStart()
          throws InterruptedException {
        permitExecution.await();
        return this.executionContext.get();
      }

      public boolean started() {
        return permitExecution.getCount() == 0;
      }
    }
  }
}
