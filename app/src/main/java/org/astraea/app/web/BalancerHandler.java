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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.EnumInfo;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.cost.ReplicaNumberCost;
import org.astraea.common.cost.ReplicaSizeCost;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.collector.Fetcher;
import org.astraea.common.metrics.collector.MetricCollector;
import org.astraea.common.metrics.collector.MetricSensor;

class BalancerHandler implements Handler {

  static final HasMoveCost DEFAULT_MOVE_COST_FUNCTIONS =
      HasMoveCost.of(
          List.of(new ReplicaNumberCost(), new ReplicaLeaderCost(), new ReplicaSizeCost()));

  private final Admin admin;
  private final RebalancePlanExecutor executor;
  private final Map<String, PostRequestWrapper> requestHistory = new ConcurrentHashMap<>();
  private final Map<String, CompletableFuture<PlanInfo>> planCalculation =
      new ConcurrentHashMap<>();
  private final Map<String, CompletableFuture<Void>> executedPlans = new ConcurrentHashMap<>();
  private final AtomicReference<String> lastExecutionId = new AtomicReference<>();
  private final Executor schedulingExecutor = Executors.newSingleThreadExecutor();
  private final Function<Integer, Optional<Integer>> jmxPortMapper;
  private final Duration sampleInterval = Duration.ofSeconds(1);

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
    this.jmxPortMapper = jmxPortMapper;
    this.executor = executor;
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    if (channel.target().isEmpty()) return CompletableFuture.completedFuture(Response.NOT_FOUND);
    var planId = channel.target().get();
    if (!planCalculation.containsKey(planId))
      return CompletableFuture.completedFuture(Response.NOT_FOUND);
    var timeout = requestHistory.get(planId).executionTime;
    var balancer = requestHistory.get(planId).balancerClasspath;
    var functions = requestHistory.get(planId).algorithmConfig.clusterCostFunction().toString();

    if (executedPlans.containsKey(planId)) {
      var f = executedPlans.get(planId);
      return CompletableFuture.completedFuture(
          new PlanExecutionProgress(
              planId,
              f.isDone() ? PlanPhase.Executed : PlanPhase.Executing,
              timeout,
              balancer,
              functions,
              f.handle((result, err) -> err != null ? err.toString() : null).getNow(null),
              planCalculation.get(planId).join().report));
    }

    var f = planCalculation.get(planId);
    return CompletableFuture.completedFuture(
        new PlanExecutionProgress(
            planId,
            f.isDone() ? PlanPhase.Searched : PlanPhase.Searching,
            timeout,
            balancer,
            functions,
            f.handle(
                    (result, err) ->
                        err != null
                            ? err.toString()
                            : result.associatedPlan.solution().isEmpty()
                                ? "Unable to propose a suitable rebalance plan"
                                : null)
                .getNow(null),
            f.handle(
                    (result, err) ->
                        err != null
                            ? null
                            : result
                                .associatedPlan
                                .solution()
                                .map(ignore -> result.report)
                                .orElse(null))
                .getNow(null)));
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    checkNoOngoingMigration();
    var balancerPostRequest = channel.request(TypeRef.of(BalancerPostRequest.class));
    var newPlanId = UUID.randomUUID().toString();
    var request =
        admin
            .topicNames(false)
            .thenCompose(admin::clusterInfo)
            .thenApply(
                currentClusterInfo ->
                    parsePostRequestWrapper(balancerPostRequest, currentClusterInfo))
            .toCompletableFuture()
            .join();
    requestHistory.put(newPlanId, request);
    var planGeneration =
        CompletableFuture.supplyAsync(
                () -> {
                  var currentClusterInfo = request.clusterInfo;
                  var fetchers =
                      Stream.concat(
                              request.algorithmConfig.clusterCostFunction().fetcher().stream(),
                              request.algorithmConfig.moveCostFunction().fetcher().stream())
                          .collect(Collectors.toUnmodifiableList());
                  var bestPlan =
                      metricContext(
                          fetchers,
                          request.algorithmConfig.clusterCostFunction().sensors(),
                          (metricSource) ->
                              Balancer.create(
                                      request.balancerClasspath,
                                      AlgorithmConfig.builder(request.algorithmConfig)
                                          .metricSource(metricSource)
                                          .build())
                                  .retryOffer(currentClusterInfo, request.executionTime));
                  var changes =
                      bestPlan
                          .solution()
                          .map(
                              p ->
                                  ClusterInfo.findNonFulfilledAllocation(
                                          currentClusterInfo, p.proposal())
                                      .stream()
                                      .map(
                                          tp ->
                                              Change.from(
                                                  currentClusterInfo.replicas(tp),
                                                  p.proposal().replicas(tp)))
                                      .collect(Collectors.toUnmodifiableList()))
                          .orElse(List.of());
                  var report =
                      new PlanReport(
                          bestPlan.initialClusterCost().value(),
                          bestPlan.solution().map(p -> p.proposalClusterCost().value()),
                          changes,
                          bestPlan
                              .solution()
                              .map(p -> migrationCosts(p.moveCost()))
                              .orElseGet(List::of));
                  return new PlanInfo(report, currentClusterInfo, bestPlan);
                })
            .whenComplete(
                (result, error) -> {
                  if (error != null)
                    new RuntimeException("Failed to generate balance plan: " + newPlanId, error)
                        .printStackTrace();
                });
    planCalculation.put(newPlanId, planGeneration.toCompletableFuture());
    return CompletableFuture.completedFuture(new PostPlanResponse(newPlanId));
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
      Collection<Fetcher> fetchers,
      Collection<MetricSensor> metricSensors,
      Function<Supplier<ClusterBean>, Balancer.Plan> execution) {
    // TODO: use a global metric collector when we are ready to enable long-run metric sampling
    //  https://github.com/skiptests/astraea/pull/955#discussion_r1026491162
    try (var collector = MetricCollector.builder().interval(sampleInterval).build()) {
      freshJmxAddresses().forEach(collector::registerJmx);
      fetchers.forEach(collector::addFetcher);
      metricSensors.forEach(collector::addMetricSensors);
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
        balancerPostRequest.timeout,
        AlgorithmConfig.builder()
            .clusterCost(balancerPostRequest.clusterCost())
            .moveCost(DEFAULT_MOVE_COST_FUNCTIONS)
            .movementConstraint(movementConstraint(balancerPostRequest))
            .topicFilter(topics::contains)
            .config(Configuration.of(balancerPostRequest.balancerConfig))
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

  @Override
  public CompletionStage<Response> put(Channel channel) {
    var request = channel.request(TypeRef.of(BalancerPutRequest.class));

    final var thePlanId = request.id;
    final var future =
        Optional.ofNullable(planCalculation.get(thePlanId))
            .orElseThrow(
                () -> new IllegalArgumentException("No such rebalance plan id: " + thePlanId));
    if (!future.isDone()) throw new IllegalStateException("No usable plan found: " + thePlanId);
    final var thePlanInfo = future.join();

    return CompletableFuture.runAsync(() -> {})
        .thenCompose(
            (ignore0) -> {
              // already scheduled, nothing to do
              if (executedPlans.containsKey(thePlanId))
                return CompletableFuture.completedFuture(new PutPlanResponse(thePlanId));

              return CompletableFuture.supplyAsync(
                      () -> {
                        checkPlanConsistency(thePlanInfo);
                        checkNoOngoingMigration();
                        // already scheduled, nothing to do
                        if (executedPlans.containsKey(thePlanId))
                          return new PutPlanResponse(thePlanId);
                        // one plan at a time
                        if (lastExecutionId.get() != null
                            && !executedPlans.get(lastExecutionId.get()).isDone())
                          throw new IllegalStateException(
                              "There is another on-going rebalance: " + lastExecutionId.get());
                        // the plan exists but no plan generated
                        if (thePlanInfo.associatedPlan.solution().isEmpty())
                          throw new IllegalStateException(
                              "The specified balancer plan didn't generate a useful plan: "
                                  + thePlanId);
                        // schedule the actual execution
                        var proposedPlan = thePlanInfo.associatedPlan.solution().get();
                        executedPlans.put(
                            thePlanId,
                            executor
                                .run(admin, proposedPlan.proposal(), Duration.ofHours(1))
                                .toCompletableFuture());
                        lastExecutionId.set(thePlanId);
                        return new PutPlanResponse(thePlanId);
                      },
                      schedulingExecutor)
                  .thenApply(x -> (Response) x)
                  .whenComplete(
                      (ignore, err) -> {
                        if (err != null)
                          new RuntimeException("Failed to execute balance plan: " + thePlanId, err)
                              .printStackTrace();
                      });
            });
  }

  private void checkPlanConsistency(PlanInfo thePlanInfo) {
    final var replicas =
        admin
            .clusterInfo(
                thePlanInfo.report.changes.stream().map(c -> c.topic).collect(Collectors.toSet()))
            .thenApply(
                clusterInfo ->
                    clusterInfo
                        .replicaStream()
                        .collect(Collectors.groupingBy(Replica::topicPartition)))
            .toCompletableFuture()
            .join();

    var mismatchPartitions =
        thePlanInfo.report.changes.stream()
            .filter(
                change -> {
                  var currentReplicaList =
                      replicas
                          .getOrDefault(
                              TopicPartition.of(change.topic, change.partition), List.of())
                          .stream()
                          .sorted(
                              Comparator.comparing(Replica::isPreferredLeader)
                                  .reversed()
                                  .thenComparing(x -> x.nodeInfo().id()))
                          .map(x -> Map.entry(x.nodeInfo().id(), x.path()))
                          .collect(Collectors.toUnmodifiableList());
                  var expectedReplicaList =
                      thePlanInfo
                          .associatedClusterInfo
                          .replicas(TopicPartition.of(change.topic, change.partition))
                          .stream()
                          // have to compare by isLeader instead of isPreferredLeader.
                          // since the leadership is what affects the direction of traffic load.
                          // Any bandwidth related cost function should calculate load by leadership
                          // instead of preferred leadership
                          .sorted(Comparator.comparing(Replica::isLeader).reversed())
                          .map(replica -> Map.entry(replica.nodeInfo().id(), replica.path()))
                          .collect(Collectors.toUnmodifiableList());
                  return !expectedReplicaList.equals(currentReplicaList);
                })
            .map(change -> TopicPartition.of(change.topic, change.partition))
            .collect(Collectors.toUnmodifiableSet());
    if (!mismatchPartitions.isEmpty())
      throw new IllegalStateException(
          "The cluster state has been changed significantly. "
              + "The following topic/partitions have different replica list(lookup the moment of plan generation): "
              + mismatchPartitions);
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

  static class PostRequestWrapper {
    final String balancerClasspath;
    final Duration executionTime;
    final AlgorithmConfig algorithmConfig;
    final ClusterInfo clusterInfo;

    PostRequestWrapper(
        String balancerClasspath,
        Duration executionTime,
        AlgorithmConfig algorithmConfig,
        ClusterInfo clusterInfo) {
      this.balancerClasspath = balancerClasspath;
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
    @JsonIgnore final double cost;

    // don't generate new cost if there is no best plan
    @JsonIgnore final Optional<Double> newCost;

    final List<Change> changes;
    final List<MigrationCost> migrationCosts;

    PlanReport(
        double initialCost,
        Optional<Double> newCost,
        List<Change> changes,
        List<MigrationCost> migrationCosts) {
      this.cost = initialCost;
      this.newCost = newCost;
      this.changes = changes;
      this.migrationCosts = migrationCosts;
    }
  }

  static class PlanInfo {
    private final PlanReport report;
    private final ClusterInfo associatedClusterInfo;
    private final Balancer.Plan associatedPlan;

    PlanInfo(PlanReport report, ClusterInfo associatedClusterInfo, Balancer.Plan associatedPlan) {
      this.report = report;
      this.associatedClusterInfo = associatedClusterInfo;
      this.associatedPlan = associatedPlan;
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
    final PlanPhase phase;
    final String exception;
    final PlanReport plan;
    final PlanConfiguration config;

    PlanExecutionProgress(
        String id,
        PlanPhase phase,
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

  enum PlanPhase implements EnumInfo {
    Searching,
    Searched,
    Executing,
    Executed;

    static PlanPhase ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(PlanPhase.class, alias);
    }

    @Override
    public String alias() {
      return name();
    }

    @Override
    public String toString() {
      return alias();
    }

    public Boolean calculated() {
      return this == Searched;
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
