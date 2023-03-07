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

import java.net.InetSocketAddress;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.BalancerConsole;
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
import org.astraea.common.metrics.collector.MetricCollector;
import org.astraea.common.metrics.collector.MetricSensor;

class BalancerHandler implements Handler {

  private final Admin admin;
  private final BalancerConsole balancerConsole;
  private final Map<String, PostRequestWrapper> taskMetadata = new ConcurrentHashMap<>();
  private final Map<String, CompletionStage<Balancer.Plan>> planGenerations =
      new ConcurrentHashMap<>();
  private final Map<String, CompletionStage<Void>> planExecutions = new ConcurrentHashMap<>();
  private final Function<Integer, Optional<Integer>> jmxPortMapper;

  BalancerHandler(Admin admin) {
    this(admin, (ignore) -> Optional.empty());
  }

  BalancerHandler(Admin admin, Function<Integer, Optional<Integer>> jmxPortMapper) {
    this.admin = admin;
    this.jmxPortMapper = jmxPortMapper;
    this.balancerConsole = BalancerConsole.create(admin);
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
      MetricCollector metricCollector = null;
      try {
        var collectorBuilder = MetricCollector.local().interval(Duration.ofSeconds(1));

        request
            .algorithmConfig
            .clusterCostFunction()
            .metricSensor()
            .ifPresent(collectorBuilder::addMetricSensor);
        request
            .algorithmConfig
            .moveCostFunction()
            .metricSensor()
            .ifPresent(collectorBuilder::addMetricSensor);
        freshJmxAddresses().forEach(collectorBuilder::registerJmx);
        metricCollector = collectorBuilder.build();
        final var mc = metricCollector;

        var task =
            balancerConsole
                .launchRebalancePlanGeneration()
                .setTaskId(taskId)
                .setBalancer(balancer)
                .setAlgorithmConfig(request.algorithmConfig)
                .setClusterBeanSource(mc::clusterBean)
                .checkNoOngoingMigration(true)
                .generate();
        task.whenComplete(
            (result, error) -> {
              if (error != null)
                new RuntimeException("Failed to generate balance plan: " + taskId, error)
                    .printStackTrace();
            });
        task.whenComplete((result, error) -> mc.close());
        taskMetadata.put(taskId, request);
        planGenerations.put(taskId, task);
      } catch (RuntimeException e) {
        if (metricCollector != null) metricCollector.close();
        throw e;
      }
      return CompletableFuture.completedFuture(new PostPlanResponse(taskId));
    }
  }

  @Override
  public CompletionStage<Response> put(Channel channel) {
    final var request = channel.request(TypeRef.of(BalancerPutRequest.class));
    final var taskId = request.id;
    final var taskPhase = balancerConsole.taskPhase(taskId);
    final var executorConfig = Configuration.of(request.executorConfig);
    final var executor =
        Utils.construct(request.executor, RebalancePlanExecutor.class, executorConfig);

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
                case Searched:
                case Executing:
                case Executed:
                  // No error message during the search & execution
                  return null;
                case SearchFailed:
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
                case ExecutionFailed:
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
        taskMetadata.get(taskId).algorithmConfig.timeout(),
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

  private Balancer.Plan metricContext(
      Collection<MetricSensor> metricSensors,
      Function<Supplier<ClusterBean>, Balancer.Plan> execution) {
    // TODO: use a global metric collector when we are ready to enable long-run metric sampling
    //  https://github.com/skiptests/astraea/pull/955#discussion_r1026491162
    try (var collector =
        MetricCollector.local()
            .registerJmxs(freshJmxAddresses())
            .addMetricSensors(metricSensors)
            .interval(Duration.ofSeconds(1))
            .build()) {
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

  // visible for test
  static PostRequestWrapper parsePostRequestWrapper(
      BalancerPostRequest balancerPostRequest, ClusterInfo currentClusterInfo) {
    var topics =
        balancerPostRequest.topics.isEmpty()
            ? currentClusterInfo.topicNames()
            : balancerPostRequest.topics;
    var costConfig = Configuration.of(balancerPostRequest.costConfig);
    var moveCost =
        HasMoveCost.of(
            List.of(
                new ReplicaNumberCost(costConfig),
                new ReplicaLeaderCost(costConfig),
                new RecordSizeCost(costConfig),
                new ReplicaLeaderSizeCost(costConfig)));

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
        AlgorithmConfig.builder()
            .clusterCost(balancerPostRequest.clusterCost())
            .moveCost(moveCost)
            .movementLimit(costConfig)
            .timeout(balancerPostRequest.timeout)
            .topicFilter(topics::contains)
            .build(),
        currentClusterInfo);
  }

  static class BalancerPostRequest implements Request {

    String balancer = GreedyBalancer.class.getName();

    Map<String, String> balancerConfig = Map.of();

    Duration timeout = Duration.ofSeconds(3);
    Set<String> topics = Set.of();
    Map<String, String> costConfig = Map.of();
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
    String id;
    String executor = StraightPlanExecutor.class.getName();
    Map<String, String> executorConfig = Map.of();
  }

  static class PostRequestWrapper {
    final String balancerClasspath;
    final Configuration balancerConfig;
    final AlgorithmConfig algorithmConfig;
    final ClusterInfo clusterInfo;

    PostRequestWrapper(
        String balancerClasspath,
        Configuration balancerConfig,
        AlgorithmConfig algorithmConfig,
        ClusterInfo clusterInfo) {
      this.balancerClasspath = balancerClasspath;
      this.balancerConfig = balancerConfig;
      this.algorithmConfig = algorithmConfig;
      this.clusterInfo = clusterInfo;
    }
  }

  static class Placement {

    final int brokerId;

    final String directory;

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
