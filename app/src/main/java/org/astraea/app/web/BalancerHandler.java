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

import com.google.gson.reflect.TypeToken;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.argument.DurationField;
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
import org.astraea.common.metrics.collector.Fetcher;
import org.astraea.common.metrics.collector.MetricCollector;

class BalancerHandler implements Handler {

  static final String TOPICS_KEY = "topics";

  static final String TIMEOUT_KEY = "timeout";
  static final String MAX_MIGRATE_SIZE_KEY = "max-migrated-size";
  static final String MAX_MIGRATE_LEADER_KEY = "max-migrated-leader";
  static final String COST_WEIGHT_KEY = "costWeights";

  static final String BALANCER_IMPLEMENTATION_KEY = "balancer";

  static final String BALANCER_CONFIGURATION_KEY = "balancer-config";

  static final int TIMEOUT_DEFAULT = 3;
  static final String BALANCER_IMPLEMENTATION_DEFAULT = GreedyBalancer.class.getName();
  static final HasClusterCost DEFAULT_CLUSTER_COST_FUNCTION =
      HasClusterCost.of(Map.of(new ReplicaSizeCost(), 1.0, new ReplicaLeaderCost(), 1.0));
  static final List<HasMoveCost> DEFAULT_MOVE_COST_FUNCTIONS =
      List.of(new ReplicaNumberCost(), new ReplicaLeaderCost(), new ReplicaSizeCost());

  private final Admin admin;
  private final RebalancePlanExecutor executor;
  private final Map<String, CompletableFuture<PlanInfo>> generatedPlans = new ConcurrentHashMap<>();
  private final Map<String, CompletableFuture<Void>> executedPlans = new ConcurrentHashMap<>();
  private final AtomicReference<String> lastExecutionId = new AtomicReference<>();
  private final Executor schedulingExecutor = Executors.newSingleThreadExecutor();
  private final Function<Integer, Optional<Integer>> jmxPortMapper;
  private final Duration sampleInterval = Duration.ofSeconds(1);

  BalancerHandler(Admin admin) {
    this(admin, (ignore) -> Optional.empty(), new StraightPlanExecutor());
  }

  BalancerHandler(Admin admin, Function<Integer, Optional<Integer>> jmxPortMapper) {
    this(admin, jmxPortMapper, new StraightPlanExecutor());
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
    if (!generatedPlans.containsKey(planId))
      return CompletableFuture.completedFuture(Response.NOT_FOUND);
    boolean isGenerated =
        generatedPlans.get(planId).isDone()
            && !generatedPlans.get(planId).isCompletedExceptionally()
            && !generatedPlans.get(planId).isCancelled();
    boolean isScheduled = executedPlans.containsKey(planId);
    boolean isDone = isScheduled && executedPlans.get(planId).isDone();
    var generationException =
        generatedPlans
            .getOrDefault(planId, CompletableFuture.completedFuture(null))
            .handle((result, error) -> error != null ? error.toString() : null)
            .getNow(null);
    var executionException =
        executedPlans
            .getOrDefault(planId, CompletableFuture.completedFuture(null))
            .handle((result, error) -> error != null ? error.toString() : null)
            .getNow(null);
    var report = isGenerated ? generatedPlans.get(planId).join().report : null;

    return CompletableFuture.completedFuture(
        new PlanExecutionProgress(
            planId,
            isGenerated,
            isScheduled,
            isDone,
            isGenerated ? executionException : generationException,
            report));
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    var newPlanId = UUID.randomUUID().toString();
    var planGeneration =
        admin
            .topicNames(false)
            .thenCompose(admin::clusterInfo)
            .thenApply(
                currentClusterInfo -> {
                  var request = parsePostRequest(channel, currentClusterInfo);
                  var fetchers =
                      Stream.concat(
                              request
                                  .configBuilder
                                  .get()
                                  .build()
                                  .clusterCostFunction()
                                  .fetcher()
                                  .stream(),
                              request.configBuilder.get().build().moveCostFunctions().stream()
                                  .flatMap(c -> c.fetcher().stream()))
                          .collect(Collectors.toUnmodifiableList());
                  var bestPlan =
                      metricContext(
                          fetchers,
                          (metricSource) ->
                              Balancer.create(
                                      request.balancerClasspath,
                                      request
                                          .configBuilder
                                          .get()
                                          .metricSource(metricSource)
                                          .build())
                                  .retryOffer(currentClusterInfo, request.executionTime));
                  var changes =
                      bestPlan
                          .map(
                              p ->
                                  ClusterInfo.findNonFulfilledAllocation(
                                          currentClusterInfo, p.proposal())
                                      .stream()
                                      .map(
                                          tp ->
                                              new Change(
                                                  tp.topic(),
                                                  tp.partition(),
                                                  // only log the size from source replicas
                                                  currentClusterInfo.replicas(tp).stream()
                                                      .map(r -> new Placement(r, r.size()))
                                                      .collect(Collectors.toList()),
                                                  p.proposal().replicas(tp).stream()
                                                      .map(r -> new Placement(r, null))
                                                      .collect(Collectors.toList())))
                                      .collect(Collectors.toUnmodifiableList()))
                          .orElse(List.of());
                  var report =
                      new Report(
                          bestPlan.map(p -> p.initialClusterCost().value()).orElse(null),
                          bestPlan.map(p -> p.proposalClusterCost().value()).orElse(null),
                          request.configBuilder.get().build().clusterCostFunction().toString(),
                          changes,
                          bestPlan
                              .map(
                                  p ->
                                      p.moveCost().stream()
                                          .map(MigrationCost::new)
                                          .collect(Collectors.toList()))
                              .orElseGet(List::of));
                  return new PlanInfo(report, bestPlan);
                })
            .whenComplete(
                (result, error) -> {
                  if (error != null)
                    new RuntimeException("Failed to generate balance plan: " + newPlanId, error)
                        .printStackTrace();
                });
    generatedPlans.put(newPlanId, planGeneration.toCompletableFuture());
    return CompletableFuture.completedFuture(new PostPlanResponse(newPlanId));
  }

  private Optional<Balancer.Plan> metricContext(
      Collection<Fetcher> fetchers,
      Function<Supplier<ClusterBean>, Optional<Balancer.Plan>> execution) {
    // TODO: use a global metric collector when we are ready to enable long-run metric sampling
    //  https://github.com/skiptests/astraea/pull/955#discussion_r1026491162
    try (var collector = MetricCollector.builder().interval(sampleInterval).build()) {
      freshJmxAddresses().forEach(collector::registerJmx);
      fetchers.forEach(collector::addFetcher);
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
  static PostRequest parsePostRequest(Channel channel, ClusterInfo<Replica> currentClusterInfo) {
    var balancerClasspath =
        channel.request().get(BALANCER_IMPLEMENTATION_KEY).orElse(BALANCER_IMPLEMENTATION_DEFAULT);
    var balancerConfig =
        channel
            .request()
            .get(BALANCER_CONFIGURATION_KEY, Map.class)
            .map(Configuration::of)
            .orElse(Configuration.of(Map.of()));
    var clusterCostFunction = getClusterCost(channel);
    var timeout =
        channel
            .request()
            .get(TIMEOUT_KEY)
            .map(DurationField::toDuration)
            .orElse(Duration.ofSeconds(TIMEOUT_DEFAULT));
    var topics =
        channel
            .request()
            .get(TOPICS_KEY)
            .map(
                s ->
                    Arrays.stream(s.split(","))
                        .filter(x -> !x.isEmpty())
                        .collect(Collectors.toSet()))
            .orElseGet(currentClusterInfo::topics);

    if (channel.request().raw().containsKey(TOPICS_KEY) && topics.isEmpty())
      throw new IllegalArgumentException(
          "Illegal topic filter, empty topic specified so nothing can be rebalance. ");
    if (timeout.isZero() || timeout.isNegative())
      throw new IllegalArgumentException(
          "Illegal timeout, value should be positive integer: " + timeout.getSeconds());

    return new PostRequest(
        balancerClasspath,
        timeout,
        () ->
            AlgorithmConfig.builder()
                .clusterCost(clusterCostFunction)
                .moveCost(DEFAULT_MOVE_COST_FUNCTIONS)
                .movementConstraint(movementConstraint(channel.request().raw()))
                .topicFilter(topics::contains)
                .config(balancerConfig));
  }

  @SuppressWarnings("unchecked")
  public static Map<HasClusterCost, Double> parseCostFunctionWeight(Configuration config) {
    return config.entrySet().stream()
        .map(
            nameAndWeight -> {
              Class<?> clz;
              try {
                clz = Class.forName(nameAndWeight.getKey());
              } catch (ClassNotFoundException ignore) {
                // this config is not cost function, so we just skip it.
                return null;
              }
              var weight = Double.parseDouble(nameAndWeight.getValue());
              if (weight < 0.0)
                throw new IllegalArgumentException("Cost-function weight should not be negative");
              return Map.entry(clz, weight);
            })
        .filter(Objects::nonNull)
        .filter(e -> HasClusterCost.class.isAssignableFrom(e.getKey()))
        .collect(
            Collectors.toMap(
                e -> Utils.construct((Class<HasClusterCost>) e.getKey(), config),
                Map.Entry::getValue));
  }

  // TODO: There needs to be a way for"GU" and Web to share this function.
  static Predicate<List<MoveCost>> movementConstraint(Map<String, String> input) {
    var converter = new DataSize.Field();
    var replicaSizeLimit =
        Optional.ofNullable(input.get(MAX_MIGRATE_SIZE_KEY)).map(x -> converter.convert(x).bytes());
    var leaderNumLimit =
        Optional.ofNullable(input.get(MAX_MIGRATE_LEADER_KEY)).map(Integer::parseInt);
    return moveCosts ->
        moveCosts.stream()
            .allMatch(
                mc -> {
                  switch (mc.name()) {
                    case ReplicaSizeCost.COST_NAME:
                      return replicaSizeLimit.filter(limit -> limit <= mc.totalCost()).isEmpty();
                    case ReplicaLeaderCost.COST_NAME:
                      return leaderNumLimit.filter(limit -> limit <= mc.totalCost()).isEmpty();
                    default:
                      return true;
                  }
                });
  }

  static HasClusterCost getClusterCost(Channel channel) {
    var costWeights =
        channel
            .request()
            .<Collection<CostWeight>>get(
                BalancerHandler.COST_WEIGHT_KEY,
                TypeToken.getParameterized(Collection.class, CostWeight.class).getType())
            .orElse(List.of());
    if (costWeights.isEmpty()) return DEFAULT_CLUSTER_COST_FUNCTION;
    costWeights.stream()
        .filter(cw -> cw.cost == null || cw.weight == null)
        .forEach(
            cw -> {
              throw new IllegalArgumentException("Malformed CostWeight specified: " + cw);
            });
    var costWeightMap =
        parseCostFunctionWeight(
            Configuration.of(
                costWeights.stream()
                    .collect(Collectors.toMap(cw -> cw.cost, cw -> String.valueOf(cw.weight)))));
    return HasClusterCost.of(costWeightMap);
  }

  @Override
  public CompletionStage<Response> put(Channel channel) {
    final var thePlanId =
        channel
            .request()
            .get("id")
            .orElseThrow(() -> new IllegalArgumentException("No rebalance plan id offered"));
    final var future =
        Optional.ofNullable(generatedPlans.get(thePlanId))
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
                        sanityCheck(thePlanInfo);
                        // already scheduled, nothing to do
                        if (executedPlans.containsKey(thePlanId))
                          return new PutPlanResponse(thePlanId);
                        if (lastExecutionId.get() != null
                            && !executedPlans.get(lastExecutionId.get()).isDone())
                          throw new IllegalStateException(
                              "There is another on-going rebalance: " + lastExecutionId.get());
                        // schedule the actual execution
                        thePlanInfo.associatedPlan.ifPresent(
                            p -> {
                              executedPlans.put(
                                  thePlanId,
                                  executor
                                      .run(admin, p.proposal(), Duration.ofHours(1))
                                      .toCompletableFuture());
                              lastExecutionId.set(thePlanId);
                            });
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

  private void sanityCheck(PlanInfo thePlanInfo) {
    final var replicas =
        admin
            .clusterInfo(
                thePlanInfo.report.changes.stream().map(c -> c.topic).collect(Collectors.toSet()))
            .thenApply(
                clusterInfo ->
                    clusterInfo
                        .replicaStream()
                        .collect(Collectors.groupingBy(ReplicaInfo::topicPartition)))
            .toCompletableFuture()
            .join();

    // sanity check: replica allocation didn't change
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
                      Stream.concat(
                              change.before.stream().limit(1),
                              change.before.stream()
                                  .skip(1)
                                  .sorted(Comparator.comparing(x -> x.brokerId)))
                          .map(x -> Map.entry(x.brokerId, x.directory))
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

    // sanity check: no ongoing migration
    var ongoingMigration =
        replicas.entrySet().stream()
            .filter(
                e ->
                    e.getValue().stream()
                        .anyMatch(r -> r.isAdding() || r.isRemoving() || r.isFuture()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableSet());
    if (!ongoingMigration.isEmpty())
      throw new IllegalStateException(
          "Another rebalance task might be working on. "
              + "The following topic/partition has ongoing migration: "
              + ongoingMigration);
  }

  static class PostRequest {
    final String balancerClasspath;
    final Duration executionTime;
    final Supplier<AlgorithmConfig.Builder> configBuilder;

    PostRequest(
        String balancerClasspath,
        Duration executionTime,
        Supplier<AlgorithmConfig.Builder> configBuilder) {
      this.balancerClasspath = balancerClasspath;
      this.executionTime = executionTime;
      this.configBuilder = configBuilder;
    }
  }

  static class Placement {

    final int brokerId;
    final String directory;

    final Long size;

    Placement(Replica replica, Long size) {
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

    Change(String topic, int partition, List<Placement> before, List<Placement> after) {
      this.topic = topic;
      this.partition = partition;
      this.before = before;
      this.after = after;
    }
  }

  static class BrokerCost {
    int brokerId;
    long cost;

    BrokerCost(int brokerId, long cost) {
      this.brokerId = brokerId;
      this.cost = cost;
    }
  }

  static class MigrationCost {
    final String function;
    final long totalCost;
    final List<BrokerCost> cost;
    final String unit;

    MigrationCost(MoveCost moveCost) {
      this.function = moveCost.name();
      this.totalCost = moveCost.totalCost();
      this.cost =
          moveCost.changes().entrySet().stream()
              .map(x -> new BrokerCost(x.getKey(), x.getValue()))
              .collect(Collectors.toList());
      this.unit = moveCost.unit();
    }
  }

  static class Report implements Response {
    // initial cost might be unavailable due to unable to evaluate cost function
    final Double cost;

    // don't generate new cost if there is no best plan
    final Double newCost;

    final String function;
    final List<Change> changes;
    final List<MigrationCost> migrationCosts;

    Report(
        Double cost,
        Double newCost,
        String function,
        List<Change> changes,
        List<MigrationCost> migrationCosts) {
      this.cost = cost;
      this.newCost = newCost;
      this.function = function;
      this.changes = changes;
      this.migrationCosts = migrationCosts;
    }
  }

  static class PlanInfo {
    private final Report report;
    private final Optional<Balancer.Plan> associatedPlan;

    PlanInfo(Report report, Optional<Balancer.Plan> associatedPlan) {
      this.report = report;
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
    final boolean generated;
    final boolean scheduled;
    final boolean done;
    final String exception;
    final Report report;

    PlanExecutionProgress(
        String id,
        boolean generated,
        boolean scheduled,
        boolean done,
        String exception,
        Report report) {
      this.id = id;
      this.generated = generated;
      this.scheduled = scheduled;
      this.done = done;
      this.exception = exception;
      this.report = report;
    }
  }

  static class CostWeight {
    final String cost;
    final Double weight;

    CostWeight(String cost, double weight) {
      this.cost = cost;
      this.weight = weight;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CostWeight that = (CostWeight) o;
      return Objects.equals(cost, that.cost) && weight == that.weight;
    }

    @Override
    public String toString() {
      return "CostWeight{" + "cost='" + cost + '\'' + ", weight=" + weight + '}';
    }
  }
}
