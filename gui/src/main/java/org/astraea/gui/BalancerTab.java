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
package org.astraea.gui;

import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.LinkedHashSet;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.generator.ShufflePlanGenerator;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.cost.ReplicaNumberCost;
import org.astraea.common.cost.ReplicaSizeCost;

public class BalancerTab {

  private enum Cost {
    REPLICA("replica", new ReplicaNumberCost()),
    LEADER("leader", new ReplicaLeaderCost()),
    SIZE("size", new ReplicaSizeCost());

    private final HasClusterCost costFunction;
    private final String alias;

    Cost(String alias, HasClusterCost costFunction) {
      this.alias = alias;
      this.costFunction = costFunction;
    }
  }

  private static List<Map<String, Object>> result(
      ClusterInfo<Replica> clusterInfo, Balancer.Plan plan) {
    return ClusterLogAllocation.findNonFulfilledAllocation(
            ClusterLogAllocation.of(clusterInfo), plan.proposal().rebalancePlan())
        .stream()
        .map(
            tp ->
                LinkedHashMap.<String, Object>of(
                    "topic",
                    tp.topic(),
                    "partition",
                    tp.partition(),
                    "old assignments",
                    clusterInfo.replicas(tp).stream()
                        .map(r -> String.valueOf(r.nodeInfo().id()))
                        .collect(Collectors.joining(",")),
                    "new assignments",
                    plan.proposal().rebalancePlan().logPlacements(tp).stream()
                        .map(r -> String.valueOf(r.nodeInfo().id()))
                        .collect(Collectors.joining(","))))
        .collect(Collectors.toList());
  }

  private static PaneBuilder.Output output(AsyncAdmin admin, PaneBuilder.Input input) {
    var clusterInfoAndPlan =
        admin
            .topicNames(true)
            .thenCompose(admin::clusterInfo)
            .thenCompose(
                clusterInfo ->
                    admin
                        .brokerFolders()
                        .thenApply(
                            brokerFolders -> {
                              input.log("searching better assignments ... ");
                              return Map.entry(
                                  clusterInfo,
                                  Balancer.builder()
                                      .planGenerator(new ShufflePlanGenerator(0, 30))
                                      .clusterCost(
                                          Arrays.stream(Cost.values())
                                              .filter(
                                                  c ->
                                                      input
                                                          .selectedRadio()
                                                          .filter(c.alias::equals)
                                                          .isPresent())
                                              .findFirst()
                                              .orElse(Cost.REPLICA)
                                              .costFunction)
                                      .limit(Duration.ofSeconds(10))
                                      .limit(10000)
                                      .greedy(true)
                                      .build()
                                      .offer(clusterInfo, input::matchSearch, brokerFolders));
                            }));

    var tableAction =
        clusterInfoAndPlan.thenApply(
            entry -> entry.getValue().map(plan -> result(entry.getKey(), plan)).orElse(List.of()));

    var messageAction =
        clusterInfoAndPlan.thenCompose(
            entry -> {
              var tpAndReplicasMap =
                  entry
                      .getValue()
                      .map(
                          plan ->
                              ClusterLogAllocation.findNonFulfilledAllocation(
                                      ClusterLogAllocation.of(entry.getKey()),
                                      plan.proposal().rebalancePlan())
                                  .stream()
                                  .collect(
                                      Collectors.toMap(
                                          Function.identity(),
                                          tp ->
                                              plan
                                                  .proposal()
                                                  .rebalancePlan()
                                                  .logPlacements(tp)
                                                  .stream()
                                                  .sorted(
                                                      Comparator.comparing(
                                                          Replica::isPreferredLeader))
                                                  .collect(Collectors.toList()))))
                      .orElse(Map.of());
              if (tpAndReplicasMap.isEmpty())
                return CompletableFuture.completedFuture("there is no better assignments");

              input.log("applying better assignments ... ");

              // TODO: how to migrate folder safely ???
              return org.astraea.common.Utils.sequence(
                      tpAndReplicasMap.entrySet().stream()
                          .map(
                              tpAndReplicas ->
                                  admin
                                      .migrator()
                                      .partition(
                                          tpAndReplicas.getKey().topic(),
                                          tpAndReplicas.getKey().partition())
                                      .moveTo(
                                          tpAndReplicas.getValue().stream()
                                              .map(r -> r.nodeInfo().id())
                                              .collect(Collectors.toList()))
                                      .whenComplete(
                                          (r, e) -> {
                                            if (e == null)
                                              input.log(
                                                  "succeed to move "
                                                      + tpAndReplicas.getKey()
                                                      + " to "
                                                      + tpAndReplicas.getValue().stream()
                                                          .map(
                                                              n ->
                                                                  String.valueOf(n.nodeInfo().id()))
                                                          .collect(Collectors.joining(",")));
                                          })
                                      .toCompletableFuture())
                          .collect(Collectors.toList()))
                  .thenApply(ignored -> "the balance is completed");
            });
    return new PaneBuilder.Output() {
      @Override
      public CompletionStage<String> message() {
        return messageAction;
      }

      @Override
      public CompletionStage<List<Map<String, Object>>> table() {
        return tableAction;
      }
    };
  }

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .radioButtons(
                LinkedHashSet.of(
                    Arrays.stream(Cost.values()).map(c -> c.alias).toArray(String[]::new)))
            .buttonName("EXECUTE")
            .searchField("topic name")
            .buttonAction(input -> context.submit(admin -> output(admin, input)))
            .build();

    var tab = new Tab("balance topic");
    tab.setContent(pane);
    return tab;
  }
}
