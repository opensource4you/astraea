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
package org.astraea.app.web;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.BalancerConsole;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;
import org.astraea.common.cost.RecordSizeCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.cost.ReplicaLeaderSizeCost;
import org.astraea.common.cost.ReplicaNumberCost;
import org.astraea.common.json.TypeRef;

class BalancerHandler implements Handler {

  static final HasMoveCost DEFAULT_MOVE_COST_FUNCTIONS =
      HasMoveCost.of(
          List.of(
              new ReplicaNumberCost(),
              new ReplicaLeaderCost(),
              new RecordSizeCost(),
              new ReplicaLeaderSizeCost()));

  private final Admin admin;
  private final BalancerConsole balancerConsole;
  private final RebalancePlanExecutor executor;
  private final Map<String, PostRequestWrapper> taskMetadata = new ConcurrentHashMap<>();
  private final Map<String, CompletionStage<Balancer.Plan>> planGenerations =
      new ConcurrentHashMap<>();
  private final Map<String, CompletionStage<Void>> planExecutions = new ConcurrentHashMap<>();

  BalancerHandler(Admin admin) {
    this(admin, (ignore) -> Optional.empty(), new StraightPlanExecutor(true));
  }

  BalancerHandler(Admin admin, Function<Integer, Optional<Integer>> jmxPortMapper) {
    this(admin, jmxPortMapper, new StraightPlanExecutor(true));
  }

  BalancerHandler(Admin admin, RebalancePlanExecutor executor) {
    this(admin, (ignore) -> Optional.empty(), executor);
  }

  BalancerHandler(
      Admin admin,
      Function<Integer, Optional<Integer>> jmxPortMapper,
      RebalancePlanExecutor executor) {
    this.admin = admin;
    this.executor = executor;
    this.balancerConsole = BalancerConsole.create(admin, jmxPortMapper);
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    if (channel.target().isEmpty()) return CompletableFuture.completedFuture(Response.NOT_FOUND);
    var taskId = channel.target().get();
    if (balancerConsole.taskPhase(taskId).isEmpty())
      return CompletableFuture.completedFuture(Response.NOT_FOUND);
    return CompletableFuture.completedFuture(progress(taskId));
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    var balancerPostRequest = channel.request(TypeRef.of(BalancerPostRequest.class));
    var request =
        admin
            .topicNames(false)
            .thenCompose(admin::clusterInfo)
            .thenApply(
                currentClusterInfo ->
                    parsePostRequestWrapper(balancerPostRequest, currentClusterInfo))
            .toCompletableFuture()
            .join();
    var balancer =
        Utils.construct(request.balancerClasspath, Balancer.class, request.balancerConfig);
    synchronized (this) {
      var taskId = UUID.randomUUID().toString();
      var task =
          balancerConsole
              .launchRebalancePlanGeneration()
              .setTaskId(taskId)
              .setBalancer(balancer)
              .setAlgorithmConfig(request.algorithmConfig)
              .setGenerationTimeout(request.executionTime)
              .checkNoOngoingMigration(true)
              .generate();
      task.whenComplete(
          (result, error) -> {
            if (error != null)
              new RuntimeException("Failed to generate balance plan: " + taskId, error)
                  .printStackTrace();
          });
      taskMetadata.put(taskId, request);
      planGenerations.put(taskId, task);
      return CompletableFuture.completedFuture(new PostPlanResponse(taskId));
    }
  }

  @Override
  public CompletionStage<Response> put(Channel channel) {
    final var request = channel.request(TypeRef.of(BalancerPutRequest.class));
    final var taskId = request.id;
    final var taskPhase = balancerConsole.taskPhase(taskId);

    if (taskPhase.isEmpty())
      throw new IllegalArgumentException("No such rebalance plan id: " + taskId);

    if (taskPhase.get() == BalancerConsole.TaskPhase.Executed)
      return CompletableFuture.completedFuture(Response.ACCEPT);

    // this method will fail if plan cannot be executed (lack of plan)
    var task =
        balancerConsole
            .launchRebalancePlanExecution()
            .setExecutor(executor)
            .setExecutionTimeout(Duration.ofHours(1))
            .checkNoOngoingMigration(true)
            .checkPlanConsistency(true)
            .execute(taskId);
    task.whenComplete(
        (ignore, error) -> {
          if (error != null)
            new RuntimeException("Failed to execute balance plan: " + taskId, error)
                .printStackTrace();
        });
    planExecutions.put(taskId, task);

    return CompletableFuture.completedFuture(new PutPlanResponse(taskId));
  }

  private PlanExecutionProgress progress(String taskId) {
    var contextCluster = taskMetadata.get(taskId).clusterInfo;
    var exception =
        (Function<BalancerConsole.TaskPhase, String>)
            (phase) -> {
              switch (phase) {
                case Searching:
                case Executing:
                  // No error message during the search & execution
                  return null;
                case Searched:
                  return planGenerations
                      .get(taskId)
                      .handle(
                          (plan, err) ->
                              err != null
                                  ? err.toString()
                                  : plan.solution().isEmpty()
                                      ? "Unable to find a balance plan that can improve the cluster"
                                      : null)
                      .toCompletableFuture()
                      .getNow(null);
                case Executed:
                  return planExecutions
                      .get(taskId)
                      .handle((ignore, err) -> err != null ? err.toString() : null)
                      .toCompletableFuture()
                      .getNow(null);
                default:
                  throw new IllegalStateException("Unknown state: " + phase);
              }
            };
    var changes =
        (Function<Balancer.Solution, List<Change>>)
            (solution) ->
                ClusterInfo.findNonFulfilledAllocation(contextCluster, solution.proposal()).stream()
                    .map(
                        tp ->
                            Change.from(
                                contextCluster.replicas(tp), solution.proposal().replicas(tp)))
                    .collect(Collectors.toUnmodifiableList());
    var moveCosts =
        (Function<Balancer.Solution, List<MigrationCost>>)
            (solution) -> migrationCosts(solution.moveCost());
    var report =
        (Supplier<PlanReport>)
            () ->
                Optional.ofNullable(
                        planGenerations
                            .get(taskId)
                            .toCompletableFuture()
                            .handle((res, err) -> res)
                            .getNow(null))
                    .flatMap(Balancer.Plan::solution)
                    .map(
                        solution ->
                            new PlanReport(changes.apply(solution), moveCosts.apply(solution)))
                    .orElse(null);
    var phase = balancerConsole.taskPhase(taskId).orElseThrow();
    return new PlanExecutionProgress(
        taskId,
        phase,
        taskMetadata.get(taskId).executionTime,
        taskMetadata.get(taskId).balancerClasspath,
        taskMetadata.get(taskId).algorithmConfig.clusterCostFunction().toString(),
        exception.apply(phase),
        report.get());
  }

  private static List<MigrationCost> migrationCosts(MoveCost cost) {
    return Stream.of(
            new MigrationCost(
                CHANGED_REPLICAS,
                cost.changedReplicaCount().entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            e -> String.valueOf(e.getKey()), e -> (double) e.getValue()))),
            new MigrationCost(
                CHANGED_LEADERS,
                cost.changedReplicaLeaderCount().entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            e -> String.valueOf(e.getKey()), e -> (double) e.getValue()))),
            new MigrationCost(
                MOVED_SIZE,
                cost.movedRecordSize().entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            e -> String.valueOf(e.getKey()), e -> (double) e.getValue().bytes()))))
        .filter(m -> !m.brokerCosts.isEmpty())
        .collect(Collectors.toList());
  }

  // visible for test
  static PostRequestWrapper parsePostRequestWrapper(
      BalancerPostRequest balancerPostRequest, ClusterInfo currentClusterInfo) {
    var topics =
        balancerPostRequest.topics.isEmpty()
            ? currentClusterInfo.topics()
            : balancerPostRequest.topics;

    if (topics.isEmpty())
      throw new IllegalArgumentException(
          "Illegal topic filter, empty topic specified so nothing can be rebalance. ");
    if (balancerPostRequest.timeout.isZero() || balancerPostRequest.timeout.isNegative())
      throw new IllegalArgumentException(
          "Illegal timeout, value should be positive integer: "
              + balancerPostRequest.timeout.getSeconds());

    return new PostRequestWrapper(
        balancerPostRequest.balancer,
        Configuration.of(balancerPostRequest.balancerConfig),
        balancerPostRequest.timeout,
        AlgorithmConfig.builder()
            .clusterCost(balancerPostRequest.clusterCost())
            .moveCost(DEFAULT_MOVE_COST_FUNCTIONS)
            .movementConstraint(movementConstraint(balancerPostRequest))
            .topicFilter(topics::contains)
            .build(),
        currentClusterInfo);
  }

  // TODO: There needs to be a way for"GU" and Web to share this function.
  static Predicate<MoveCost> movementConstraint(BalancerPostRequest request) {
    return cost -> {
      if (request.maxMigratedSize.bytes()
          < cost.movedRecordSize().values().stream().mapToLong(DataSize::bytes).sum()) return false;
      if (request.maxMigratedLeader
          < cost.changedReplicaLeaderCount().values().stream().mapToLong(s -> s).sum())
        return false;
      return true;
    };
  }

  static class BalancerPostRequest implements Request {

    String balancer = GreedyBalancer.class.getName();

    Map<String, String> balancerConfig = Map.of();

    Duration timeout = Duration.ofSeconds(3);
    Set<String> topics = Set.of();

    DataSize maxMigratedSize = DataSize.Byte.of(Long.MAX_VALUE);

    long maxMigratedLeader = Long.MAX_VALUE;

    List<CostWeight> costWeights = List.of();

    HasClusterCost clusterCost() {
      if (costWeights.isEmpty()) throw new IllegalArgumentException("costWeights is not specified");
      var config =
          Configuration.of(
              costWeights.stream()
                  .collect(Collectors.toMap(e -> e.cost, e -> String.valueOf(e.weight))));
      var fs = Utils.costFunctions(config, HasClusterCost.class);
      if (fs.size() != costWeights.size())
        throw new IllegalArgumentException(
            "Has invalid costs: "
                + costWeights.stream()
                    .filter(
                        e ->
                            fs.keySet().stream()
                                .map(c -> c.getClass().getName())
                                .noneMatch(n -> e.cost.equals(n)))
                    .map(e -> e.cost)
                    .collect(Collectors.joining(",")));
      return HasClusterCost.of(fs);
    }
  }

  static class CostWeight implements Request {
    String cost;
    double weight = 1.D;
  }

  static class BalancerPutRequest implements Request {
    private String id;
  }

  static class PostRequestWrapper {
    final String balancerClasspath;
    final Configuration balancerConfig;
    final Duration executionTime;
    final AlgorithmConfig algorithmConfig;
    final ClusterInfo clusterInfo;

    PostRequestWrapper(
        String balancerClasspath,
        Configuration balancerConfig,
        Duration executionTime,
        AlgorithmConfig algorithmConfig,
        ClusterInfo clusterInfo) {
      this.balancerClasspath = balancerClasspath;
      this.balancerConfig = balancerConfig;
      this.executionTime = executionTime;
      this.algorithmConfig = algorithmConfig;
      this.clusterInfo = clusterInfo;
    }
  }

  static class Placement {

    final int brokerId;

    // temporarily disable data-directory migration, there are some Kafka bug related to it.
    // see https://github.com/skiptests/astraea/issues/1325#issue-1506582838
    @JsonIgnore final String directory;

    final Optional<Long> size;

    Placement(Replica replica, Optional<Long> size) {
      this.brokerId = replica.nodeInfo().id();
      this.directory = replica.path();
      this.size = size;
    }
  }

  static class Change {
    final String topic;
    final int partition;
    final List<Placement> before;
    final List<Placement> after;

    static Change from(Collection<Replica> before, Collection<Replica> after) {
      if (before.size() == 0) throw new NoSuchElementException("Empty replica list was given");
      if (after.size() == 0) throw new NoSuchElementException("Empty replica list was given");
      var tp = before.stream().findAny().orElseThrow().topicPartition();
      if (!before.stream().allMatch(r -> r.topicPartition().equals(tp)))
        throw new IllegalArgumentException("Some replica come from different topic/partition");
      if (!after.stream().allMatch(r -> r.topicPartition().equals(tp)))
        throw new IllegalArgumentException("Some replica come from different topic/partition");
      return new Change(
          tp.topic(),
          tp.partition(),
          // only log the size from source replicas
          before.stream()
              .sorted(Comparator.comparing(Replica::isPreferredLeader).reversed())
              .map(r -> new Placement(r, Optional.of(r.size())))
              .collect(Collectors.toList()),
          after.stream()
              .sorted(Comparator.comparing(Replica::isPreferredLeader).reversed())
              .map(r -> new Placement(r, Optional.empty()))
              .collect(Collectors.toList()));
    }

    Change(String topic, int partition, List<Placement> before, List<Placement> after) {
      this.topic = topic;
      this.partition = partition;
      this.before = before;
      this.after = after;
    }
  }

  // visible for testing
  static final String CHANGED_REPLICAS = "changed replicas";
  static final String CHANGED_LEADERS = "changed leaders";
  static final String MOVED_SIZE = "moved size (bytes)";

  static class MigrationCost {
    final String name;

    final Map<String, Double> brokerCosts;

    MigrationCost(String name, Map<String, Double> brokerCosts) {
      this.name = name;
      this.brokerCosts = brokerCosts;
    }
  }

  static class PlanReport implements Response {

    final List<Change> changes;
    final List<MigrationCost> migrationCosts;

    PlanReport(List<Change> changes, List<MigrationCost> migrationCosts) {
      this.changes = changes;
      this.migrationCosts = migrationCosts;
    }
  }

  static class PostPlanResponse implements Response {
    final String id;

    PostPlanResponse(String id) {
      this.id = id;
    }
  }

  static class PutPlanResponse implements Response {
    final String id;

    PutPlanResponse(String id) {
      this.id = id;
    }

    @Override
    public int code() {
      return Response.ACCEPT.code();
    }
  }

  static class PlanExecutionProgress implements Response {
    final String id;
    final BalancerConsole.TaskPhase phase;
    final String exception;
    final PlanReport plan;
    final PlanConfiguration config;

    PlanExecutionProgress(
        String id,
        BalancerConsole.TaskPhase phase,
        Duration timeout,
        String balancer,
        String function,
        String exception,
        PlanReport plan) {
      this.id = id;
      this.phase = phase;
      this.exception = exception;
      this.plan = plan;
      this.config = new PlanConfiguration(balancer, function, timeout);
    }
  }

  static class PlanConfiguration implements Response {
    final String balancer;

    final String function;

    final Duration timeout;

    PlanConfiguration(String balancer, String function, Duration timeout) {
      this.balancer = balancer;
      this.function = function;
      this.timeout = timeout;
    }
  }
}
