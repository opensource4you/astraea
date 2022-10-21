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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.MapUtils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.generator.ShufflePlanGenerator;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.cost.ReplicaNumberCost;
import org.astraea.common.cost.ReplicaSizeCost;
import org.astraea.gui.Context;
import org.astraea.gui.Logger;
import org.astraea.gui.pane.Input;
import org.astraea.gui.pane.PaneBuilder;
import org.astraea.gui.pane.Tab;

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
              var oldAssignments = clusterInfo.replicas(tp);
              var newAssignments = plan.proposal().rebalancePlan().logPlacements(tp);
              var previousLeader =
                  oldAssignments.stream().filter(ReplicaInfo::isLeader).findFirst().get();
              var newLeader =
                  newAssignments.stream().filter(ReplicaInfo::isLeader).findFirst().get();
              var previousFollowers =
                  oldAssignments.stream().filter(r -> !r.isLeader()).collect(Collectors.toList());
              var newFollowers =
                  newAssignments.stream().filter(r -> !r.isLeader()).collect(Collectors.toList());
              var migratedReplicas = diff(oldAssignments, newAssignments);
              return MapUtils.<String, Object>of(
                  TOPIC_NAME_KEY,
                  tp.topic(),
                  PARTITION_KEY,
                  tp.partition(),
                  PREVIOUS_LEADER_KEY,
                  previousLeader.nodeInfo().id() + ":" + previousLeader.path(),
                  NEW_LEADER_KEY,
                  migratedReplicas.stream().anyMatch(ReplicaInfo::isLeader)
                      ? newLeader.nodeInfo().id() + ":" + newLeader.path()
                      : "",
                  PREVIOUS_FOLLOWER_KEY,
                  previousFollowers.size() == 0
                      ? ""
                      : previousFollowers.stream()
                          .map(r -> r.nodeInfo().id() + ":" + r.path())
                          .collect(Collectors.joining(",")),
                  NEW_FOLLOWER_KEY,
                  migratedReplicas.stream().anyMatch(r -> !r.isLeader())
                      ? newFollowers.stream()
                          .map(r -> r.nodeInfo().id() + ":" + r.path())
                          .collect(Collectors.joining(","))
                      : "");
            })
        .collect(Collectors.toList());
  }

  private static List<Replica> diff(Collection<Replica> before, Collection<Replica> after) {
    return before.stream()
        .filter(
            beforeReplica ->
                after.stream()
                    .noneMatch(
                        r ->
                            r.nodeInfo().id() == beforeReplica.nodeInfo().id()
                                && r.topic().equals(beforeReplica.topic())
                                && r.partition() == beforeReplica.partition()
                                && r.path().equals(beforeReplica.path())))
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
                          logger.log("searching better assignments ... ");
                          return Map.entry(
                              clusterInfo,
                              Balancer.builder()
                                  .planGenerator(new ShufflePlanGenerator(0, 30))
                                  .clusterCost(
                                      HasClusterCost.of(
                                          input
                                              .multiSelectedRadios(Arrays.asList(Cost.values()))
                                              .stream()
                                              .map(cost -> Map.entry(cost.costFunction, 1.0))
                                              .collect(
                                                  Collectors.toMap(
                                                      Map.Entry::getKey, Map.Entry::getValue))))
                                  .limit(Duration.ofSeconds(10))
                                  .limit(10000)
                                  .greedy(true)
                                  .build()
                                  .offer(clusterInfo, input::matchSearch, brokerFolders));
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
            .multiRadioButtons(Arrays.stream(Cost.values()).collect(Collectors.toList()))
            .buttonName("PLAN")
            .searchField("topic name", "topic-*,*abc*")
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
            .buttonAction((input, logger) -> generator(context, input, logger))
            .build();

    return Tab.of("balancer", pane);
  }
}
