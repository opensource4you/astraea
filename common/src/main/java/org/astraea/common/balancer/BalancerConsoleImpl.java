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

  private final Map<String, TaskPhase> taskPhases = new ConcurrentHashMap<>();
  private final Map<String, BalanceTaskImpl> tasks = new ConcurrentHashMap<>();
  private final AtomicReference<BalanceTaskImpl> lastExecutingTask = new AtomicReference<>();

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  public BalancerConsoleImpl(Admin admin, Function<Integer, Optional<Integer>> jmxPortMapper) {
    this.admin = admin;
    this.jmxPortMapper = jmxPortMapper;
  }

  @Override
  public Set<String> tasks() {
    return Collections.unmodifiableSet(taskPhases.keySet());
  }

  @Override
  public Optional<TaskPhase> taskPhase(String taskId) {
    return Optional.ofNullable(taskPhases.get(taskId));
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
        final var taskId = Objects.requireNonNull(this.taskId);
        final var balancer = Objects.requireNonNull(this.balancer);
        final var timeout = Objects.requireNonNull(this.timeout);
        final var config = Objects.requireNonNull(this.algorithmConfig);
        final var sensors =
            Stream.concat(
                    config.clusterCostFunction().metricSensor().stream(),
                    config.moveCostFunction().metricSensor().stream())
                .collect(Collectors.toUnmodifiableList());
        if (this.checkNoOngoingMigration) BalancerConsoleImpl.this.checkNoOngoingMigration();
        synchronized (this) {
          checkNotClosed();
          if (tasks().contains(taskId))
            throw new IllegalStateException("Conflict task ID: " + taskId);
          var sourceCluster =
              admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();
          taskPhases.put(taskId, TaskPhase.Searching);
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
                                  })))
                      .whenComplete((plan, err) -> taskPhases.put(taskId, TaskPhase.Searched))));
        }
        return tasks.get(taskId).planGeneration.minimalCompletionStage();
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
        if (!tasks.containsKey(taskId))
          throw new IllegalArgumentException("No such balance task: " + taskId);
        var task = tasks.get(taskId);
        if (this.checkNoOngoingMigration) BalancerConsoleImpl.this.checkNoOngoingMigration();
        if (this.checkPlanConsistency) BalancerConsoleImpl.this.checkPlanConsistency(task);
        synchronized (this) {
          checkNotClosed();
          // If this plan already executed, raise an exception
          if (taskPhases.get(taskId) == TaskPhase.Executing) return tasks.get(taskId).planExecution;
          if (taskPhases.get(taskId) == TaskPhase.Executed)
            return CompletableFuture.completedStage(null);
          // another task is still running
          if (lastExecutingTask.get() != null && taskPhases.get(taskId) != TaskPhase.Executed)
            throw new IllegalStateException(
                "Another task is executing: " + lastExecutingTask.get().taskId);
          // start the execution. this should fail if the plan is not ready
          task.startExecution(
              (targetClusterInfo) -> {
                taskPhases.put(taskId, TaskPhase.Executing);
                return executor
                    .run(admin, targetClusterInfo, timeout)
                    .whenComplete(
                        (ignore, err) -> {
                          // intentionally remove this task from internal, so it might be GC later.
                          tasks.remove(taskId);
                          // mark this task as executed
                          taskPhases.put(taskId, TaskPhase.Executed);
                        });
              });
          lastExecutingTask.set(task);
        }
        return task.planExecution.minimalCompletionStage();
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
              .filter(task -> taskPhases.get(task.taskId) == TaskPhase.Searching)
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
          lastExecutingTask.get().planExecution.toCompletableFuture().get();
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
            .clusterInfo(task.sourceCluster.topicNames())
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

  private static final class BalanceTaskImpl {

    private final String taskId;
    private final CompletableFuture<Balancer.Plan> planGeneration;
    private final CompletableFuture<Function<ClusterInfo, CompletionStage<Void>>> executionLatch;
    private final CompletableFuture<Void> planExecution;
    private final ClusterInfo sourceCluster;

    private BalanceTaskImpl(
        String taskId, ClusterInfo sourceCluster, CompletableFuture<Balancer.Plan> planGeneration) {
      this.taskId = taskId;
      this.sourceCluster = sourceCluster;
      this.planGeneration = planGeneration;
      this.executionLatch = new CompletableFuture<>();
      this.planExecution =
          this.planGeneration.thenCompose(
              plan ->
                  this.executionLatch.thenCompose(
                      context ->
                          plan.solution()
                              .map(Balancer.Solution::proposal)
                              .map(context)
                              .orElseThrow(
                                  () ->
                                      new IllegalStateException(
                                          "Plan generation failed to find any usable balance plan that will improve this cluster: "
                                              + taskId))));
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
      if (executionLatch.isDone())
        throw new IllegalStateException("This rebalance execution already started");
      if (planGeneration.isDone()
          && !planGeneration.isCancelled()
          && !planGeneration.isCompletedExceptionally()
          && planGeneration.getNow(null).solution().isEmpty())
        throw new IllegalStateException(
            "Plan generation failed to find any usable balance plan that will improve this cluster: "
                + taskId);

      executionLatch.complete(executionContext);
    }
  }
}
