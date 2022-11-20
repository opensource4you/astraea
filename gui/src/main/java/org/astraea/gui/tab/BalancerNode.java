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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javafx.scene.Node;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.MoveCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.cost.ReplicaNumberCost;
import org.astraea.common.cost.ReplicaSizeCost;
import org.astraea.common.function.Bi3Function;
import org.astraea.gui.Context;
import org.astraea.gui.Logger;
import org.astraea.gui.button.SelectBox;
import org.astraea.gui.pane.Argument;
import org.astraea.gui.pane.MultiInput;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.TableRefresher;
import org.astraea.gui.table.TableViewer;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.TextInput;

public class BalancerNode {

  static final AtomicReference<Balancer.Plan> LAST_PLAN = new AtomicReference<>();
  static final String TOPIC_NAME_KEY = "topic";
  private static final String PARTITION_KEY = "partition";
  private static final String MAX_MIGRATE_LOG_SIZE = "total max migrate log size";
  static final String MAX_MIGRATE_LEADER_NUM = "maximum leader number to migrate";
  private static final String PREVIOUS_LEADER_KEY = "previous leader";
  private static final String NEW_LEADER_KEY = "new leader";
  private static final String PREVIOUS_FOLLOWER_KEY = "previous follower";
  private static final String NEW_FOLLOWER_KEY = "new follower";

  enum Cost {
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
    public String toString() {
      return display;
    }
  }

  static List<Map<String, Object>> costResult(Balancer.Plan plan) {
    return plan.moveCost().stream()
        .map(
            cost -> {
              var result = new LinkedHashMap<String, Object>();
              result.put("name", cost.name());
              result.put(
                  "total cost",
                  cost.name().equals(ReplicaSizeCost.COST_NAME)
                      ? DataSize.Byte.of(cost.totalCost())
                      : cost.totalCost());
              cost.changes().entrySet().stream()
                  .sorted(Map.Entry.comparingByKey())
                  .forEach(
                      entry ->
                          result.put(
                              "broker " + entry.getKey(),
                              cost.name().equals(ReplicaSizeCost.COST_NAME)
                                  ? DataSize.Byte.of(entry.getValue())
                                  : entry.getValue()));
              return result;
            })
        .collect(Collectors.toList());
  }

  static List<Map<String, Object>> assignmentResult(
      ClusterInfo<Replica> clusterInfo, Balancer.Plan plan) {
    return ClusterInfo.findNonFulfilledAllocation(clusterInfo, plan.proposal()).stream()
        .map(
            tp -> {
              var previousAssignments = clusterInfo.replicas(tp);
              var newAssignments = plan.proposal().replicas(tp);
              var result = new LinkedHashMap<String, Object>();
              result.put(TOPIC_NAME_KEY, tp.topic());
              result.put(PARTITION_KEY, tp.partition());
              previousAssignments.stream()
                  .filter(ReplicaInfo::isLeader)
                  .findFirst()
                  .ifPresent(
                      r -> result.put(PREVIOUS_LEADER_KEY, r.nodeInfo().id() + ":" + r.path()));
              newAssignments.stream()
                  .filter(ReplicaInfo::isLeader)
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

  static Predicate<List<MoveCost>> movementConstraint(Map<String, String> input) {
    var converter = new DataSize.Field();
    var replicaSizeLimit =
        Optional.ofNullable(input.get(MAX_MIGRATE_LOG_SIZE)).map(x -> converter.convert(x).bytes());
    var leaderNumLimit =
        Optional.ofNullable(input.get(MAX_MIGRATE_LEADER_NUM)).map(Integer::parseInt);
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

  static TableRefresher refresher(Context context) {
    return (argument, logger) ->
        context
            .admin()
            .topicNames(false)
            .thenCompose(context.admin()::clusterInfo)
            .thenApply(
                clusterInfo -> {
                  var patterns =
                      argument
                          .texts()
                          .get(TOPIC_NAME_KEY)
                          .map(
                              ss ->
                                  Arrays.stream(ss.split(","))
                                      .map(Utils::wildcardToPattern)
                                      .collect(Collectors.toList()))
                          .orElse(List.of());
                  logger.log("searching better assignments ... ");
                  return Map.entry(
                      clusterInfo,
                      Balancer.create(
                              GreedyBalancer.class,
                              AlgorithmConfig.builder()
                                  .clusterCost(
                                      HasClusterCost.of(clusterCosts(argument.selectedKeys())))
                                  .moveCost(List.of(new ReplicaSizeCost(), new ReplicaLeaderCost()))
                                  .movementConstraint(movementConstraint(argument.nonEmptyTexts()))
                                  .topicFilter(
                                      topic ->
                                          patterns.isEmpty()
                                              || patterns.stream()
                                                  .anyMatch(p -> p.matcher(topic).matches()))
                                  .config("iteration", "10000")
                                  .build())
                          .offer(clusterInfo, Duration.ofSeconds(10)));
                })
            .thenApply(
                entry -> {
                  entry.getValue().ifPresent(LAST_PLAN::set);
                  var result =
                      entry
                          .getValue()
                          .map(
                              plan ->
                                  Map.of(
                                      "assignment",
                                      assignmentResult(entry.getKey(), plan),
                                      "cost",
                                      costResult(plan)))
                          .orElse(Map.of());
                  if (result.isEmpty()) logger.log("there is no better assignments");
                  else logger.log("find a better assignments!!!!");
                  return result;
                });
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

  public static Node of(Context context) {
    var selectBox =
        SelectBox.multi(
            Arrays.stream(Cost.values()).map(Cost::toString).collect(Collectors.toList()),
            Cost.values().length);
    var multiInput =
        MultiInput.of(
            List.of(
                TextInput.of(
                    TOPIC_NAME_KEY, EditableText.singleLine().hint("topic-*,*abc*").build()),
                TextInput.of(
                    MAX_MIGRATE_LEADER_NUM, EditableText.singleLine().onlyNumber().build()),
                TextInput.of(
                    MAX_MIGRATE_LOG_SIZE,
                    EditableText.singleLine().hint("30KB,200MB,1GB").build())));
    return PaneBuilder.of(TableViewer.disableQuery())
        .firstPart(selectBox, multiInput, "PLAN", refresher(context))
        .secondPart(null, "EXECUTE", tableViewAction(context))
        .build();
  }
}
