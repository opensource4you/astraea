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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.balancer.BalancerUtils;
import org.astraea.app.balancer.RebalancePlanProposal;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.cost.HasClusterCost;
import org.astraea.app.cost.HasMoveCost;
import org.astraea.app.cost.ReplicaSizeCost;

class BalancerHandler implements Handler {

  static String LIMIT_KEY = "limit";

  static String TOPICS_KEY = "topics";

  static int LIMIT_DEFAULT = 10000;
  private final Admin admin;
  private final RebalancePlanGenerator generator = RebalancePlanGenerator.random(30);
  final HasClusterCost costFunction;
  final HasMoveCost moveCostFunction;

  BalancerHandler(Admin admin) {
    this(admin, new ReplicaSizeCost(), new ReplicaSizeCost());
  }

  BalancerHandler(Admin admin, HasClusterCost costFunction, HasMoveCost moveCostFunction) {
    this.admin = admin;
    this.costFunction = costFunction;
    this.moveCostFunction = moveCostFunction;
  }

  @Override
  public Response get(Channel channel) {
    var clusterInfo = admin.clusterInfo();
    var cost = costFunction.clusterCost(clusterInfo, ClusterBean.EMPTY).value();
    var topics =
        Optional.ofNullable(channel.queries().get(TOPICS_KEY))
            .map(s -> (Set<String>) new HashSet<>(Arrays.asList(s.split(","))))
            .orElseGet(() -> admin.topicNames(false));
    var limit =
        Integer.parseInt(channel.queries().getOrDefault(LIMIT_KEY, String.valueOf(LIMIT_DEFAULT)));
    // generate migration plans only for specify topics
    var targetAllocations = ClusterLogAllocation.of(admin.clusterInfo(topics));
    var planAndCost =
        generator
            .generate(admin.brokerFolders(), targetAllocations)
            .limit(limit)
            .map(RebalancePlanProposal::rebalancePlan)
            .map(
                cla ->
                    Map.entry(
                        cla,
                        costFunction
                            .clusterCost(
                                BalancerUtils.update(ClusterInfo.EMPTY_REPLICA, cla),
                                ClusterBean.EMPTY)
                            .value()))
            .filter(e -> e.getValue() <= cost)
            .min(Comparator.comparingDouble(Map.Entry::getValue));
    var cla = planAndCost.orElse(Map.entry(targetAllocations, 0.0)).getKey();
    var newClusterInfo = BalancerUtils.update(clusterInfo, cla);
    var moveCost = moveCostFunction.moveCost(clusterInfo, newClusterInfo, ClusterBean.EMPTY);
    var migrateInfos =
        moveCost.changes().entrySet().stream()
            .map(e -> new MigrateInfo(e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    var moveCosts =
        List.of(
            new Migration(moveCost.name(), moveCost.totalCost(), migrateInfos, moveCost.unit()));
    return new Report(
        cost,
        planAndCost.map(Map.Entry::getValue).orElse(cost),
        limit,
        costFunction.getClass().getSimpleName(),
        planAndCost
            .map(
                entry ->
                    ClusterLogAllocation.findNonFulfilledAllocation(
                            targetAllocations, entry.getKey())
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
                                            clusterInfo
                                                .replica(
                                                    TopicPartitionReplica.of(
                                                        tp.topic(), tp.partition(), l.broker()))
                                                .map(Replica::size)
                                                .orElse(null)),
                                    placements(entry.getKey().logPlacements(tp), ignored -> null)))
                        .collect(Collectors.toUnmodifiableList()))
            .orElse(List.of()),
        moveCosts);
  }

  static List<Placement> placements(List<LogPlacement> lps, Function<LogPlacement, Long> size) {
    return lps.stream()
        .map(p -> new Placement(p, size.apply(p)))
        .collect(Collectors.toUnmodifiableList());
  }

  static class Placement {

    final int brokerId;
    final String directory;

    final Long size;

    Placement(LogPlacement lp, Long size) {
      this.brokerId = lp.broker();
      this.directory = lp.dataFolder();
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

  static class MigrateInfo {
    int broker;
    long cost;

    MigrateInfo(int broker, long cost) {
      this.broker = broker;
      this.cost = cost;
    }
  }

  static class Migration {
    final String function;
    final long totalCost;
    final List<MigrateInfo> cost;
    final String unit;

    Migration(String function, long totalCost, List<MigrateInfo> cost, String unit) {
      this.function = function;
      this.totalCost = totalCost;
      this.cost = cost;
      this.unit = unit;
    }
  }

  static class Report implements Response {
    final double cost;
    final double newCost;
    final int limit;
    final String function;
    final List<Change> changes;
    final List<Migration> migrations;

    Report(
        double cost,
        double newCost,
        int limit,
        String function,
        List<Change> changes,
        List<Migration> migrations) {
      this.cost = cost;
      this.newCost = newCost;
      this.limit = limit;
      this.function = function;
      this.changes = changes;
      this.migrations = migrations;
    }
  }
}
