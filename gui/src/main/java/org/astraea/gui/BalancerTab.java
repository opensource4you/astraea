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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.Partition;
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
    REPLICA(new ReplicaNumberCost()),
    LEADER(new ReplicaLeaderCost()),
    SIZE(new ReplicaSizeCost());

    final HasClusterCost costFunction;

    Cost(HasClusterCost costFunction) {
      this.costFunction = costFunction;
    }
  }

  public static Tab of(Context context) {
    var tab = new Tab("balance topic");
    var cost = Utils.radioButton(Cost.values());
    BiFunction<String, Console, CompletionStage<SearchResult<Balancer.Plan>>> planGenerator =
        (word, console) ->
            context.submit(
                admin ->
                    admin
                        .topicNames(true)
                        .thenApply(
                            names ->
                                names.stream()
                                    .filter(name -> word.isEmpty() || name.contains(word))
                                    .collect(Collectors.toSet()))
                        .thenCompose(admin::partitions)
                        .thenCompose(
                            partitions -> {
                              var topics =
                                  partitions.stream()
                                      .map(Partition::topic)
                                      .collect(Collectors.toSet());
                              return admin
                                  .clusterInfo(topics)
                                  .thenCompose(
                                      clusterInfo -> {
                                        console.append(
                                            "start to generate optimized assignments for topics: "
                                                + topics);
                                        return admin
                                            .brokerFolders()
                                            .thenApply(
                                                brokerFolders -> {
                                                  var optionalPlan =
                                                      Balancer.builder()
                                                          .planGenerator(
                                                              new ShufflePlanGenerator(0, 30))
                                                          .clusterCost(
                                                              cost.entrySet().stream()
                                                                  .filter(
                                                                      e ->
                                                                          e.getValue().isSelected())
                                                                  .map(Map.Entry::getKey)
                                                                  .findFirst()
                                                                  .orElse(Cost.REPLICA)
                                                                  .costFunction)
                                                          .limit(Duration.ofSeconds(10))
                                                          .limit(10000)
                                                          .greedy(true)
                                                          .build()
                                                          .offer(
                                                              clusterInfo,
                                                              topics::contains,
                                                              brokerFolders);
                                                  if (optionalPlan.isEmpty())
                                                    return SearchResult.empty();
                                                  var plan = optionalPlan.get();
                                                  var allocation = plan.proposal().rebalancePlan();
                                                  var items =
                                                      ClusterLogAllocation
                                                          .findNonFulfilledAllocation(
                                                              ClusterLogAllocation.of(clusterInfo),
                                                              allocation)
                                                          .stream()
                                                          .map(
                                                              tp ->
                                                                  LinkedHashMap.<String, Object>of(
                                                                      "topic",
                                                                      tp.topic(),
                                                                      "partition",
                                                                      tp.partition(),
                                                                      "old assignments",
                                                                      clusterInfo
                                                                          .replicas(tp)
                                                                          .stream()
                                                                          .map(
                                                                              r ->
                                                                                  r.nodeInfo().id()
                                                                                      + ":"
                                                                                      + r
                                                                                          .dataFolder())
                                                                          .collect(
                                                                              Collectors.joining(
                                                                                  ",")),
                                                                      "new assignments",
                                                                      allocation
                                                                          .logPlacements(tp)
                                                                          .stream()
                                                                          .map(
                                                                              r ->
                                                                                  r.nodeInfo().id()
                                                                                      + ":"
                                                                                      + r
                                                                                          .dataFolder())
                                                                          .collect(
                                                                              Collectors.joining(
                                                                                  ","))))
                                                          .collect(Collectors.toList());
                                                  return SearchResult.of(items, plan);
                                                });
                                      });
                            }));

    BiFunction<SearchResult<Balancer.Plan>, Console, CompletionStage<List<Void>>> planExecutor =
        (result, console) ->
            context.submit(
                admin ->
                    admin
                        .topicNames(true)
                        .thenCompose(admin::clusterInfo)
                        .thenCompose(
                            clusterInfo -> {
                              var allocation = result.object().proposal().rebalancePlan();
                              var changedPartitions =
                                  ClusterLogAllocation.findNonFulfilledAllocation(
                                      ClusterLogAllocation.of(clusterInfo), allocation);
                              var tpAndReplicas =
                                  changedPartitions.stream()
                                      .collect(
                                          Collectors.toMap(
                                              Function.identity(),
                                              tp ->
                                                  allocation.logPlacements(tp).stream()
                                                      .sorted(
                                                          Comparator.comparing(
                                                              Replica::isPreferredLeader))
                                                      .collect(Collectors.toList())));

                              return org.astraea.common.Utils.sequence(
                                      tpAndReplicas.entrySet().stream()
                                          .map(
                                              entry -> {
                                                var tp = entry.getKey();
                                                var replicas = entry.getValue();
                                                return admin
                                                    .migrator()
                                                    .partition(tp.topic(), tp.partition())
                                                    .moveTo(
                                                        replicas.stream()
                                                            .map(r -> r.nodeInfo().id())
                                                            .collect(Collectors.toList()))
                                                    .toCompletableFuture();
                                              })
                                          .collect(Collectors.toList()))
                                  .thenCompose(
                                      ignored -> {
                                        org.astraea.common.Utils.sleep(Duration.ofSeconds(5));
                                        return org.astraea.common.Utils.sequence(
                                            tpAndReplicas.entrySet().stream()
                                                .map(
                                                    entry2 -> {
                                                      var tp = entry2.getKey();
                                                      var replicas = entry2.getValue();
                                                      return admin
                                                          .migrator()
                                                          .partition(tp.topic(), tp.partition())
                                                          .moveTo(
                                                              replicas.stream()
                                                                  .collect(
                                                                      Collectors.toMap(
                                                                          r -> r.nodeInfo().id(),
                                                                          Replica::dataFolder)))
                                                          .whenComplete(
                                                              (r, e) -> {
                                                                if (e == null)
                                                                  console.append(
                                                                      "start to migrate " + tp);
                                                                else
                                                                  console.append(
                                                                      "failed to migrate "
                                                                          + tp
                                                                          + " due to "
                                                                          + e.getMessage());
                                                              })
                                                          .toCompletableFuture();
                                                    })
                                                .collect(Collectors.toList()));
                                      });
                            }));

    tab.setContent(
        Utils.searchToTable(
            planGenerator,
            planExecutor,
            Stream.concat(Stream.of(new Label("balanced by:")), cost.values().stream())
                .collect(Collectors.toList())));
    return tab;
  }
}
