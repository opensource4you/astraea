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
package org.astraea.gui.tab;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.LinkedHashMap;
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
import org.astraea.gui.Context;
import org.astraea.gui.Logger;
import org.astraea.gui.button.RadioButtonAble;
import org.astraea.gui.pane.Input;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

public class BalancerTab {

  private enum Cost implements RadioButtonAble {
    REPLICA("replica", new ReplicaNumberCost()),
    LEADER("leader", new ReplicaLeaderCost()),
    SIZE("size", new ReplicaSizeCost());

    private final HasClusterCost costFunction;
    private final String display;

    Cost(String display, HasClusterCost costFunction) {
      this.display = display;
      this.costFunction = costFunction;
    }

    @Override
    public String display() {
      return display;
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

  private static CompletionStage<List<Map<String, Object>>> generator(
      AsyncAdmin admin, Input input, Logger logger) {
    return admin
        .topicNames(true)
        .thenCompose(admin::clusterInfo)
        .thenCompose(
            clusterInfo ->
                admin
                    .brokerFolders()
                    .thenApply(
                        brokerFolders -> {
                          logger.log("searching better assignments ... ");
                          return Map.entry(
                              clusterInfo,
                              Balancer.builder()
                                  .planGenerator(new ShufflePlanGenerator(0, 30))
                                  .clusterCost(
                                      input
                                          .selectedRadio()
                                          .map(o -> (Cost) o)
                                          .orElse(Cost.REPLICA)
                                          .costFunction)
                                  .limit(Duration.ofSeconds(10))
                                  .limit(10000)
                                  .greedy(true)
                                  .build()
                                  .offer(clusterInfo, input::matchSearch, brokerFolders));
                        }))
        .thenCompose(
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
              if (tpAndReplicasMap.isEmpty()) {
                logger.log("there is no better assignments");
                return CompletableFuture.completedFuture(List.of());
              }
              logger.log("applying better assignments ... ");
              // TODO: how to migrate folder safely ???
              return admin
                  .moveToBrokers(
                      tpAndReplicasMap.entrySet().stream()
                          .collect(
                              Collectors.toMap(
                                  Map.Entry::getKey,
                                  e ->
                                      e.getValue().stream()
                                          .map(r -> r.nodeInfo().id())
                                          .collect(Collectors.toList()))))
                  .thenApply(
                      ignored ->
                          entry
                              .getValue()
                              .map(plan -> result(entry.getKey(), plan))
                              .orElse(List.of()));
            });
  }

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .radioButtons(Cost.values())
            .buttonName("EXECUTE")
            .searchField("topic name")
            .buttonAction(
                (input, logger) -> context.submit(admin -> generator(admin, input, logger)))
            .build();

    return Tab.of("balance topic", pane);
  }
}
