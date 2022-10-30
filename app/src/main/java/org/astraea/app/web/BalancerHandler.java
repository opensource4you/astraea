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
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.FutureUtils;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.argument.DurationField;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
import org.astraea.common.cost.Configuration;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.cost.ReplicaSizeCost;

class BalancerHandler implements Handler {

  static final String LOOP_KEY = "loop";

  static final String TOPICS_KEY = "topics";

  static final String TIMEOUT_KEY = "timeout";

  static final String COST_WEIGHT_KEY = "costWeights";

  static final int LOOP_DEFAULT = 10000;
  static final int TIMEOUT_DEFAULT = 3;
  static final HasClusterCost DEFAULT_CLUSTER_COST_FUNCTION =
      HasClusterCost.of(Map.of(new ReplicaSizeCost(), 1.0, new ReplicaLeaderCost(), 1.0));

  private final Admin admin;
  private final RebalancePlanExecutor executor;
  final HasMoveCost moveCostFunction;
  private final Map<String, CompletableFuture<PlanInfo>> generatedPlans = new ConcurrentHashMap<>();
  private final Map<String, CompletableFuture<Void>> executedPlans = new ConcurrentHashMap<>();
  private final AtomicReference<String> lastExecutionId = new AtomicReference<>();

  BalancerHandler(Admin admin) {
    this(admin, new ReplicaSizeCost());
  }

  BalancerHandler(Admin admin, HasMoveCost moveCostFunction) {
    this(admin, moveCostFunction, new StraightPlanExecutor());
  }

  BalancerHandler(Admin admin, HasMoveCost moveCostFunction, RebalancePlanExecutor executor) {
    this.admin = admin;
    this.moveCostFunction = moveCostFunction;
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
    var clusterCostFunction = getClusterCost(channel);
    var planGeneration =
        FutureUtils.combine(
            admin.topicNames(false).thenCompose(admin::clusterInfo),
            admin.brokerFolders(),
            (currentClusterInfo, brokerFolders) -> {
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
                      .map(s -> (Set<String>) new HashSet<>(Arrays.asList(s.split(","))))
                      .orElseGet(currentClusterInfo::topics);
              var cost =
                  clusterCostFunction.clusterCost(currentClusterInfo, ClusterBean.EMPTY).value();
              var loop =
                  Integer.parseInt(
                      channel.request().get(LOOP_KEY).orElse(String.valueOf(LOOP_DEFAULT)));
              var bestPlan =
                  Balancer.create(
                          SingleStepBalancer.class,
                          AlgorithmConfig.builder()
                              .clusterCost(clusterCostFunction)
                              .moveCost(List.of(moveCostFunction))
                              .topicFilter(topics::contains)
                              .limit(loop)
                              .limit(timeout)
                              .build())
                      .offer(currentClusterInfo, brokerFolders);
              var changes =
                  bestPlan
                      .map(
                          p ->
                              ClusterInfo.findNonFulfilledAllocation(
                                      currentClusterInfo, p.proposal().rebalancePlan())
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
                                              p.proposal().rebalancePlan().replicas(tp).stream()
                                                  .map(r -> new Placement(r, null))
                                                  .collect(Collectors.toList())))
                                  .collect(Collectors.toUnmodifiableList()))
                      .orElse(List.of());
              var report =
                  new Report(
                      newPlanId,
                      cost,
                      bestPlan.map(p -> p.clusterCost().value()).orElse(null),
                      loop,
                      bestPlan.map(p -> p.proposal().index()).orElse(null),
                      clusterCostFunction.getClass().getSimpleName(),
                      changes,
                      bestPlan
                          .map(p -> List.of(new MigrationCost(p.moveCost().iterator().next())))
                          .orElseGet(List::of));
              return new PlanInfo(report, bestPlan);
            });
    generatedPlans.put(newPlanId, planGeneration.toCompletableFuture());
    return CompletableFuture.completedFuture(new PostPlanResponse(newPlanId));
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

  HasClusterCost getClusterCost(Channel channel) {
    var costWeights =
        channel
            .request()
            .<Collection<CostWeight>>get(
                BalancerHandler.COST_WEIGHT_KEY,
                TypeToken.getParameterized(Collection.class, CostWeight.class).getType())
            .orElse(List.of());
    if (costWeights.isEmpty()) return DEFAULT_CLUSTER_COST_FUNCTION;
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

    return sanityCheck(thePlanInfo)
        .handle(
            (r, e) -> {
              synchronized (this) {
                // already scheduled, nothing to do
                if (executedPlans.containsKey(thePlanId)) return new PutPlanResponse(thePlanId);
                if (lastExecutionId.get() != null
                    && !executedPlans.get(lastExecutionId.get()).isDone())
                  throw new IllegalStateException(
                      "There are another on-going rebalance: " + lastExecutionId.get());
                // the plan is eligible for execution
                if (e != null) throw (RuntimeException) e;
                // schedule the actual execution
                thePlanInfo.associatedPlan.ifPresent(
                    p -> {
                      executedPlans.put(
                          thePlanId,
                          CompletableFuture.runAsync(
                              () -> executor.run(admin, p.proposal().rebalancePlan())));
                      lastExecutionId.set(thePlanId);
                    });
                return new PutPlanResponse(thePlanId);
              }
            });
  }

  private CompletionStage<Void> sanityCheck(PlanInfo thePlanInfo) {
    return FutureUtils.combine(
        admin
            .replicas(
                thePlanInfo.report.changes.stream().map(c -> c.topic).collect(Collectors.toSet()))
            .thenApply(
                replicas ->
                    replicas.stream().collect(Collectors.groupingBy(ReplicaInfo::topicPartition))),
        admin.topicNames(false).thenCompose(admin::addingReplicas),
        (replicas, addingReplicas) -> {
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
              addingReplicas.stream()
                  .map(replica -> TopicPartition.of(replica.topic(), replica.partition()))
                  .collect(Collectors.toUnmodifiableSet());
          if (!ongoingMigration.isEmpty())
            throw new IllegalStateException(
                "Another rebalance task might be working on. "
                    + "The following topic/partition has ongoing migration: "
                    + ongoingMigration);
          return null;
        });
  }

  static List<Placement> placements(Collection<Replica> lps, Function<Replica, Long> size) {
    return lps.stream()
        .sorted(Comparator.comparing(Replica::isPreferredLeader).reversed())
        .map(p -> new Placement(p, size.apply(p)))
        .collect(Collectors.toUnmodifiableList());
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
    final String id;
    final double cost;

    // don't generate new cost if there is no best plan
    final Double newCost;
    final int limit;

    // don't generate step if there is no best plan
    final Integer step;
    final String function;
    final List<Change> changes;
    final List<MigrationCost> migrationCosts;

    Report(
        String id,
        double cost,
        Double newCost,
        int limit,
        Integer step,
        String function,
        List<Change> changes,
        List<MigrationCost> migrationCosts) {
      this.id = id;
      this.cost = cost;
      this.newCost = newCost;
      this.limit = limit;
      this.step = step;
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
    final double weight;

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
  }
}
