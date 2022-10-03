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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.generator.RebalancePlanGenerator;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;
import org.astraea.common.cost.ReplicaSizeCost;

class BalancerHandler implements Handler {

  // TODO: implement an endpoint to execute rebalance plan, see
  // https://github.com/skiptests/astraea/issues/743

  static String LIMIT_KEY = "limit";

  static String TOPICS_KEY = "topics";

  static int LIMIT_DEFAULT = 10000;
  private final Admin admin;
  private final RebalancePlanGenerator generator = RebalancePlanGenerator.random(30);
  final HasClusterCost clusterCostFunction;
  final HasMoveCost moveCostFunction;
  final Map<UUID, PlanInfo> generatedPlans = new ConcurrentHashMap<>();

  BalancerHandler(Admin admin) {
    this(admin, new ReplicaSizeCost(), new ReplicaSizeCost());
  }

  BalancerHandler(Admin admin, HasClusterCost clusterCostFunction, HasMoveCost moveCostFunction) {
    this.admin = admin;
    this.clusterCostFunction = clusterCostFunction;
    this.moveCostFunction = moveCostFunction;
  }

  @Override
  public Response get(Channel channel) {
    var topics =
        Optional.ofNullable(channel.queries().get(TOPICS_KEY))
            .map(s -> (Set<String>) new HashSet<>(Arrays.asList(s.split(","))))
            .orElseGet(() -> admin.topicNames(false));
    var currentClusterInfo = admin.clusterInfo();
    var cost = clusterCostFunction.clusterCost(currentClusterInfo, ClusterBean.EMPTY).value();
    var limit =
        Integer.parseInt(channel.queries().getOrDefault(LIMIT_KEY, String.valueOf(LIMIT_DEFAULT)));
    var targetAllocations = ClusterLogAllocation.of(admin.clusterInfo(topics));
    var bestPlan =
        Balancer.builder()
            .planGenerator(generator)
            .clusterCost(clusterCostFunction)
            .moveCost(moveCostFunction)
            .limit(LIMIT_DEFAULT)
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
    var id = bestPlan.map(ignore -> UUID.randomUUID()).orElse(null);
    var report =
        new Report(
            id,
            cost,
            bestPlan.map(p -> p.clusterCost().value()).orElse(null),
            limit,
            bestPlan.map(p -> p.proposal().index()).orElse(null),
            clusterCostFunction.getClass().getSimpleName(),
            changes,
            bestPlan.map(p -> List.of(new MigrationCost(p.moveCost()))).orElseGet(List::of));
    bestPlan.ifPresent(thePlan -> generatedPlans.put(id, new PlanInfo(report, thePlan)));
    return report;
  }

  static List<Placement> placements(Set<Replica> lps, Function<Replica, Long> size) {
    return lps.stream()
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
    final UUID id;
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
        UUID id,
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

    Report report() {
      return report;
    }

    Balancer.Plan plan() {
      return associatedPlan;
    }
  }
}
