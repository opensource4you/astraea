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
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.cost.ReplicaNumberCost;
import org.astraea.common.cost.ReplicaSizeCost;
import org.astraea.gui.Context;
import org.astraea.gui.Logger;
import org.astraea.gui.button.SelectBox;
import org.astraea.gui.pane.Input;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;
import org.astraea.gui.text.EditableText;
import org.astraea.gui.text.NoneditableText;

public class BalancerTab {

  private static final String TOPIC_NAME_KEY = "topic";
  private static final String PARTITION_KEY = "partition";
  private static final String PREVIOUS_LEADER_KEY = "previous leader";
  private static final String NEW_LEADER_KEY = "new leader";
  private static final String PREVIOUS_FOLLOWER_KEY = "previous follower";
  private static final String NEW_FOLLOWER_KEY = "new follower";
  private static final String NEW_ASSIGNMENT_KEY = "new assignments";

  private enum Cost {
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

  private static List<Map<String, Object>> result(
      ClusterInfo<Replica> clusterInfo, Balancer.Plan plan) {
    return ClusterLogAllocation.findNonFulfilledAllocation(
            ClusterLogAllocation.of(clusterInfo), plan.proposal().rebalancePlan())
        .stream()
        .map(
            tp -> {
              var previousAssignments = clusterInfo.replicas(tp);
              var newAssignments = plan.proposal().rebalancePlan().logPlacements(tp);
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

  private static CompletionStage<List<Map<String, Object>>> generator(
      Context context, Input input, Logger logger) {
    return context
        .admin()
        .topicNames(false)
        .thenCompose(context.admin()::clusterInfo)
        .thenCompose(
            clusterInfo ->
                context
                    .admin()
                    .brokerFolders()
                    .thenApply(
                        brokerFolders -> {
                          var patterns =
                              input
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
                                              HasClusterCost.of(
                                                  input.selectedKeys().stream()
                                                      .flatMap(
                                                          name ->
                                                              Arrays.stream(Cost.values())
                                                                  .filter(
                                                                      c ->
                                                                          c.toString()
                                                                              .equals(name)))
                                                      .map(
                                                          cost -> Map.entry(cost.costFunction, 1.0))
                                                      .collect(
                                                          Collectors.toMap(
                                                              Map.Entry::getKey,
                                                              Map.Entry::getValue))))
                                          .limit(Duration.ofSeconds(10))
                                          .limit(10000)
                                          .build())
                                  .offer(
                                      clusterInfo,
                                      topic ->
                                          patterns.isEmpty()
                                              || patterns.stream()
                                                  .anyMatch(p -> p.matcher(topic).matches()),
                                      brokerFolders));
                        }))
        .thenApply(
            entry -> {
              var result =
                  entry.getValue().map(plan -> result(entry.getKey(), plan)).orElse(List.of());
              if (result.isEmpty()) logger.log("there is no better assignments");
              else
                logger.log(
                    "find a better assignments. Total number of reassignments is " + result.size());
              return result;
            });
  }

  public static Tab of(Context context) {
    var pane =
        PaneBuilder.of()
            .selectBox(
                SelectBox.multi(
                    Arrays.stream(Cost.values()).map(Cost::toString).collect(Collectors.toList()),
                    Cost.values().length))
            .clickName("PLAN")
            .input(
                NoneditableText.of(TOPIC_NAME_KEY),
                EditableText.singleLine().hint("topic-*,*abc*").build())
            .tableViewAction(
                Map.of(),
                "EXECUTE",
                (items, inputs, logger) -> {
                  logger.log("applying better assignments ... ");
                  var reassignments =
                      items.stream()
                          .flatMap(
                              item -> {
                                var topic = item.get(TOPIC_NAME_KEY);
                                var partition = item.get(PARTITION_KEY);
                                var assignments = item.get(NEW_ASSIGNMENT_KEY);
                                if (topic != null && partition != null && assignments != null)
                                  return Stream.of(
                                      Map.entry(
                                          TopicPartition.of(
                                              topic.toString(),
                                              Integer.parseInt(partition.toString())),
                                          Arrays.stream(assignments.toString().split(","))
                                              .map(
                                                  assignment ->
                                                      Map.entry(
                                                          Integer.parseInt(
                                                              assignment.split(":")[0]),
                                                          assignment.split(":")[1]))
                                              .collect(Collectors.toList())));
                                return Stream.of();
                              })
                          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                  var moveBrokerRequest =
                      reassignments.entrySet().stream()
                          .collect(
                              Collectors.toMap(
                                  Map.Entry::getKey,
                                  e ->
                                      e.getValue().stream()
                                          .map(Map.Entry::getKey)
                                          .collect(Collectors.toList())));
                  var moveFolderRequest =
                      reassignments.entrySet().stream()
                          .flatMap(
                              e ->
                                  e.getValue().stream()
                                      .map(
                                          idAndPath ->
                                              Map.entry(
                                                  TopicPartitionReplica.of(
                                                      e.getKey().topic(),
                                                      e.getKey().partition(),
                                                      idAndPath.getKey()),
                                                  idAndPath.getValue())))
                          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                  var expectedReplicas =
                      moveBrokerRequest.entrySet().stream()
                          .flatMap(
                              e ->
                                  e.getValue().stream()
                                      .map(
                                          id ->
                                              TopicPartitionReplica.of(
                                                  e.getKey().topic(), e.getKey().partition(), id)))
                          .collect(Collectors.toList());
                  return context
                      .admin()
                      .moveToBrokers(moveBrokerRequest)
                      // wait assignment
                      .thenCompose(
                          ignored ->
                              context
                                  .admin()
                                  .waitCluster(
                                      moveBrokerRequest.keySet().stream()
                                          .map(TopicPartition::topic)
                                          .collect(Collectors.toSet()),
                                      clusterInfo ->
                                          expectedReplicas.stream()
                                              .allMatch(
                                                  r ->
                                                      clusterInfo
                                                          .replicaStream()
                                                          .anyMatch(
                                                              replica ->
                                                                  replica
                                                                      .topicPartitionReplica()
                                                                      .equals(r))),
                                      Duration.ofSeconds(15),
                                      2))
                      .thenCompose(ignored -> context.admin().moveToFolders(moveFolderRequest))
                      .thenAccept(ignored -> logger.log("succeed to balance cluster"));
                })
            .tableRefresher((input, logger) -> generator(context, input, logger))
            .build();

    return Tab.of("balancer", pane);
  }
}
