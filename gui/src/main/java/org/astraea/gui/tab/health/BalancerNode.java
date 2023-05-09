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
package org.astraea.gui.tab.health;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javafx.scene.Node;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.BalancerConfigs;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MigrationCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.cost.ReplicaLeaderSizeCost;
import org.astraea.common.cost.ReplicaNumberCost;
import org.astraea.common.function.Bi3Function;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.gui.Context;
import org.astraea.gui.Logger;
import org.astraea.gui.button.SelectBox;
import org.astraea.gui.pane.Argument;
import org.astraea.gui.pane.FirstPart;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.SecondPart;
import org.astraea.gui.table.TableViewer;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

class BalancerNode {

  static final AtomicReference<Balancer.Plan> LAST_PLAN = new AtomicReference<>();
  static final String TOPIC_NAME_KEY = "topic";
  private static final String PARTITION_KEY = "partition";
  private static final String PREVIOUS_LEADER_KEY = "previous leader";
  private static final String NEW_LEADER_KEY = "new leader";
  private static final String PREVIOUS_FOLLOWER_KEY = "previous follower";
  private static final String NEW_FOLLOWER_KEY = "new follower";

  enum Cost {
    REPLICA("replica", new ReplicaNumberCost()),
    LEADER("leader", new ReplicaLeaderCost()),
    SIZE("size", new ReplicaLeaderSizeCost());

    private final HasClusterCost costFunction;
    private final String display;

    Cost(String display, HasClusterCost costFunction) {
      this.display = display;
      this.costFunction = costFunction;
    }

    @Override
    public String toString() {
      return display;
    }
  }

  static List<Map<String, Object>> costResult(Balancer.Plan plan) {
    var map = new HashMap<Integer, LinkedHashMap<String, Object>>();

    Consumer<List<MigrationCost>> process =
        (migrationCosts) ->
            migrationCosts.forEach(
                migrationCost ->
                    migrationCost.brokerCosts.forEach(
                        (id, count) ->
                            map.computeIfAbsent(
                                    id,
                                    ignored -> {
                                      var r = new LinkedHashMap<String, Object>();
                                      r.put("id", id);
                                      return r;
                                    })
                                .put(migrationCost.name, count)));

    process.accept(MigrationCost.migrationCosts(plan.initialClusterInfo(), plan.proposal()));
    return List.copyOf(map.values());
  }

  static List<Map<String, Object>> assignmentResult(Balancer.Plan plan) {
    return ClusterInfo.findNonFulfilledAllocation(plan.initialClusterInfo(), plan.proposal())
        .stream()
        .map(
            tp -> {
              var previousAssignments = plan.initialClusterInfo().replicas(tp);
              var newAssignments = plan.proposal().replicas(tp);
              var result = new LinkedHashMap<String, Object>();
              result.put(TOPIC_NAME_KEY, tp.topic());
              result.put(PARTITION_KEY, tp.partition());
              previousAssignments.stream()
                  .filter(Replica::isLeader)
                  .findFirst()
                  .ifPresent(
                      r -> result.put(PREVIOUS_LEADER_KEY, r.nodeInfo().id() + ":" + r.path()));
              newAssignments.stream()
                  .filter(Replica::isLeader)
                  .findFirst()
                  .ifPresent(r -> result.put(NEW_LEADER_KEY, r.nodeInfo().id() + ":" + r.path()));
              var previousFollowers =
                  previousAssignments.stream()
                      .filter(r -> !r.isLeader())
                      .map(r -> r.nodeInfo().id() + ":" + r.path())
                      .collect(Collectors.joining(","));
              var newFollowers =
                  newAssignments.stream()
                      .filter(r -> !r.isLeader())
                      .map(r -> r.nodeInfo().id() + ":" + r.path())
                      .collect(Collectors.joining(","));
              if (!previousFollowers.isBlank())
                result.put(PREVIOUS_FOLLOWER_KEY, previousFollowers);
              if (!newFollowers.isBlank()) result.put(NEW_FOLLOWER_KEY, newFollowers);
              return result;
            })
        .collect(Collectors.toList());
  }

  static Map<HasClusterCost, Double> clusterCosts(List<String> keys) {
    return keys.stream()
        .flatMap(
            name -> Arrays.stream(Cost.values()).filter(c -> c.toString().equalsIgnoreCase(name)))
        .map(cost -> Map.entry(cost.costFunction, 1.0))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static Bi3Function<List<Map<String, Object>>, Argument, Logger, CompletionStage<Void>>
      tableViewAction(Context context) {
    return (items, inputs, logger) -> {
      var plan = LAST_PLAN.getAndSet(null);
      if (plan != null) {
        logger.log("applying better assignments ... ");
        var f =
            RebalancePlanExecutor.of()
                .run(context.admin(), plan.proposal(), Duration.ofHours(2))
                .toCompletableFuture();
        return CompletableFuture.runAsync(
                () -> {
                  while (!f.isDone()) {
                    context
                        .admin()
                        .topicNames(false)
                        .thenCompose(context.admin()::clusterInfo)
                        .whenComplete(
                            (clusterInfo, e) -> {
                              if (clusterInfo != null) {
                                var count =
                                    clusterInfo
                                        .replicaStream()
                                        .filter(r -> r.isFuture() || r.isAdding() || r.isRemoving())
                                        .count();
                                if (count > 0)
                                  logger.log(
                                      "There are " + count + " moving replicas ... please wait");
                              }
                            });
                    // TODO: should we make this sleep configurable?
                    Utils.sleep(Duration.ofSeconds(7));
                  }
                })
            .thenCompose(ignored -> f)
            .thenAccept(ignored -> logger.log("succeed to execute re-balance"));
      }
      logger.log("Please click \"PLAN\" to generate re-balance plan");
      return CompletableFuture.completedFuture(null);
    };
  }

  static BiFunction<Argument, Logger, CompletionStage<Map<String, List<Map<String, Object>>>>>
      refresher(Context context) {
    return (argument, logger) ->
        context
            .admin()
            .topicNames(false)
            .thenCompose(context.admin()::clusterInfo)
            .thenApply(
                clusterInfo -> {
                  var pattern =
                      argument
                          .texts()
                          .get(TOPIC_NAME_KEY)
                          .map(
                              ss ->
                                  Arrays.stream(ss.split(","))
                                      .map(Utils::wildcardToPattern)
                                      .map(Pattern::pattern)
                                      .collect(Collectors.joining("|", "(", ")")))
                          .orElse(".*");
                  logger.log("searching better assignments ... ");
                  return Utils.construct(
                          GreedyBalancer.class,
                          Configuration.of(Map.of(GreedyBalancer.ITERATION_CONFIG, "10000")))
                      .offer(
                          AlgorithmConfig.builder()
                              .clusterInfo(clusterInfo)
                              .clusterBean(ClusterBean.EMPTY)
                              .timeout(Duration.ofSeconds(10))
                              .clusterCost(HasClusterCost.of(clusterCosts(argument.selectedKeys())))
                              .moveCost(
                                  HasMoveCost.of(
                                      List.of(
                                          new ReplicaLeaderSizeCost(), new ReplicaLeaderCost())))
                              .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, pattern)
                              .build());
                })
            .thenApply(
                optionalPlan -> {
                  optionalPlan.ifPresent(LAST_PLAN::set);
                  var result =
                      optionalPlan
                          .map(
                              plan ->
                                  Map.of(
                                      "assignment",
                                      assignmentResult(plan),
                                      "cost",
                                      costResult(plan)))
                          .orElse(Map.of());
                  if (result.isEmpty()) logger.log("there is no better assignments");
                  else
                    logger.log(
                        "find a plan with improvement from "
                            + optionalPlan.get().initialClusterCost().value()
                            + " to "
                            + optionalPlan.get().proposalClusterCost().value());
                  return result;
                });
  }

  public static Node of(Context context) {
    var selectBox =
        SelectBox.multi(
            Arrays.stream(Cost.values()).map(Cost::toString).collect(Collectors.toList()),
            Cost.values().length);
    var multiInput =
        List.of(
            TextInput.of(TOPIC_NAME_KEY, EditableText.singleLine().hint("topic-*,*abc*").build()));
    var firstPart =
        FirstPart.builder()
            .selectBox(selectBox)
            .textInputs(multiInput)
            .clickName("PLAN")
            .tablesRefresher(refresher(context))
            .build();
    var secondPart =
        SecondPart.builder().buttonName("EXECUTE").action(tableViewAction(context)).build();
    return PaneBuilder.of(TableViewer.disableQuery())
        .firstPart(firstPart)
        .secondPart(secondPart)
        .build();
  }
}
