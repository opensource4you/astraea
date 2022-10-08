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

import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.argument.DurationField;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.executor.RebalanceAdmin;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
import org.astraea.common.balancer.generator.RebalancePlanGenerator;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;
import org.astraea.common.cost.ReplicaSizeCost;

class BalancerHandler implements Handler {

  static final String LOOP_KEY = "loop";

  static final String TOPICS_KEY = "topics";

  static final String TIMEOUT_KEY = "timeout";

  static final int LOOP_DEFAULT = 10000;
  static final int TIMEOUT_DEFAULT = 3;

  private final Admin admin;
  private final RebalancePlanGenerator generator;
  private final RebalancePlanExecutor executor;
  final HasClusterCost clusterCostFunction;
  final HasMoveCost moveCostFunction;
  private final Map<String, PlanInfo> generatedPlans = new ConcurrentHashMap<>();
  private final Map<String, CompletableFuture<Void>> executedPlans = new ConcurrentHashMap<>();
  private final AtomicReference<String> lastExecutionId = new AtomicReference<>();

  BalancerHandler(Admin admin) {
    this(admin, new ReplicaSizeCost(), new ReplicaSizeCost());
  }

  BalancerHandler(Admin admin, HasClusterCost clusterCostFunction, HasMoveCost moveCostFunction) {
    this(
        admin,
        clusterCostFunction,
        moveCostFunction,
        RebalancePlanGenerator.random(30),
        new StraightPlanExecutor());
  }

  BalancerHandler(
      Admin admin,
      HasClusterCost clusterCostFunction,
      HasMoveCost moveCostFunction,
      RebalancePlanGenerator generator,
      RebalancePlanExecutor executor) {
    this.admin = admin;
    this.clusterCostFunction = clusterCostFunction;
    this.moveCostFunction = moveCostFunction;
    this.generator = generator;
    this.executor = executor;
  }

  @Override
  public Response get(Channel channel) {
    return channel
        .target()
        .map(this::lookupRebalancePlanProgress)
        .orElseGet(() -> searchRebalancePlan(channel));
  }

  private Response searchRebalancePlan(Channel channel) {
    var timeout =
        Optional.ofNullable(channel.queries().get(TIMEOUT_KEY))
            .map(DurationField::toDuration)
            .orElse(Duration.ofSeconds(TIMEOUT_DEFAULT));
    var topics =
        Optional.ofNullable(channel.queries().get(TOPICS_KEY))
            .map(s -> (Set<String>) new HashSet<>(Arrays.asList(s.split(","))))
            .orElseGet(() -> admin.topicNames(false));
    var currentClusterInfo = admin.clusterInfo();
    var cost = clusterCostFunction.clusterCost(currentClusterInfo, ClusterBean.EMPTY).value();
    var loop =
        Integer.parseInt(channel.queries().getOrDefault(LOOP_KEY, String.valueOf(LOOP_DEFAULT)));
    var targetAllocations = ClusterLogAllocation.of(admin.clusterInfo(topics));
    var bestPlan =
        Balancer.builder()
            .planGenerator(generator)
            .clusterCost(clusterCostFunction)
            .moveCost(moveCostFunction)
            .limit(loop)
            .limit(timeout)
            .build()
            .offer(currentClusterInfo, topics::contains, admin.brokerFolders());
    var changes =
        bestPlan
            .map(
                p ->
                    ClusterLogAllocation.findNonFulfilledAllocation(
                            targetAllocations, p.proposal().rebalancePlan())
                        .stream()
                        .map(
                            tp ->
                                new Change(
                                    tp.topic(),
                                    tp.partition(),
                                    // only log the size from source replicas
                                    placements(
                                        targetAllocations.logPlacements(tp),
                                        l ->
                                            currentClusterInfo
                                                .replica(
                                                    TopicPartitionReplica.of(
                                                        tp.topic(),
                                                        tp.partition(),
                                                        l.nodeInfo().id()))
                                                .map(Replica::size)
                                                .orElse(null)),
                                    placements(
                                        p.proposal().rebalancePlan().logPlacements(tp),
                                        ignored -> null)))
                        .collect(Collectors.toUnmodifiableList()))
            .orElse(List.of());
    var id = bestPlan.map(ignore -> UUID.randomUUID()).map(UUID::toString).orElse(null);
    var report =
        new Report(
            id,
            cost,
            bestPlan.map(p -> p.clusterCost().value()).orElse(null),
            loop,
            bestPlan.map(p -> p.proposal().index()).orElse(null),
            clusterCostFunction.getClass().getSimpleName(),
            changes,
            bestPlan.map(p -> List.of(new MigrationCost(p.moveCost()))).orElseGet(List::of));
    bestPlan.ifPresent(thePlan -> generatedPlans.put(id, new PlanInfo(report, thePlan)));
    return report;
  }

  private Response lookupRebalancePlanProgress(String planId) {
    if (!generatedPlans.containsKey(planId))
      throw new IllegalArgumentException("This plan doesn't exists: " + planId);
    boolean isScheduled = executedPlans.containsKey(planId);
    boolean isDone = isScheduled && executedPlans.get(planId).isDone();
    boolean isException = isScheduled && executedPlans.get(planId).isCompletedExceptionally();

    return new PlanExecutionProgress(planId, isScheduled, isDone, isException);
  }

  @Override
  public Response put(Channel channel) {
    final var thePlanId =
        channel
            .request()
            .get("id")
            .orElseThrow(() -> new IllegalArgumentException("No rebalance plan id offered"));
    final var thePlanInfo =
        Optional.ofNullable(generatedPlans.get(thePlanId))
            .orElseThrow(
                () -> new IllegalArgumentException("No such rebalance plan id: " + thePlanId));
    final var theRebalanceProposal = thePlanInfo.associatedPlan.proposal();

    synchronized (this) {
      if (executedPlans.containsKey(thePlanId)) {
        // already scheduled, nothing to do
        return new PutPlanResponse(thePlanId);
      } else if (lastExecutionId.get() != null
          && !executedPlans.get(lastExecutionId.get()).isDone()) {
        throw new IllegalStateException(
            "There are another on-going rebalance: " + lastExecutionId.get());
      } else {
        // check if the plan is eligible for execution
        sanityCheck(thePlanInfo);

        // schedule the actual execution
        executedPlans.put(
            thePlanId,
            CompletableFuture.runAsync(
                () ->
                    executor.run(RebalanceAdmin.of(admin), theRebalanceProposal.rebalancePlan())));
        lastExecutionId.set(thePlanId);
        return new PutPlanResponse(thePlanId);
      }
    }
  }

  private void sanityCheck(PlanInfo thePlanInfo) {
    // sanity check: replica allocation didn't change
    final var mismatchPartitions =
        thePlanInfo.report.changes.stream()
            .filter(
                change -> {
                  var currentReplicaList =
                      admin.replicas(Set.of(change.topic)).stream()
                          .filter(replica -> replica.partition() == change.partition)
                          .sorted(
                              Comparator.comparing(Replica::isPreferredLeader)
                                  .reversed()
                                  .thenComparing(x -> x.nodeInfo().id()))
                          .map(x -> Map.entry(x.nodeInfo().id(), x.dataFolder()))
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
        admin.addingReplicas(admin.topicNames()).stream()
            .map(replica -> TopicPartition.of(replica.topic(), replica.partition()))
            .collect(Collectors.toUnmodifiableSet());
    if (!ongoingMigration.isEmpty())
      throw new IllegalStateException(
          "Another rebalance task might be working on. "
              + "The following topic/partition has ongoing migration: "
              + ongoingMigration);
  }

  static List<Placement> placements(Set<Replica> lps, Function<Replica, Long> size) {
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
      this.directory = replica.dataFolder();
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
    private final Balancer.Plan associatedPlan;

    PlanInfo(Report report, Balancer.Plan associatedPlan) {
      this.report = report;
      this.associatedPlan = associatedPlan;
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
    final boolean scheduled;
    final boolean done;
    final boolean exception;

    PlanExecutionProgress(String id, boolean scheduled, boolean done, boolean exception) {
      this.id = id;
      this.scheduled = scheduled;
      this.done = done;
      this.exception = exception;
    }
  }
}
