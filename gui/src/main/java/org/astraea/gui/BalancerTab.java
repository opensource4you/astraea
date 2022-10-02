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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.generator.ShufflePlanGenerator;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.cost.ReplicaLeaderCost;

public class BalancerTab {

  public static Tab of(Context context) {
    var tab = new Tab("balance topic");
    var lastPlan = new AtomicReference<Balancer.Plan>();
    var console = new Console("");
    var executePlanButton = new SafeButton("apply");
    var search =
        Utils.searchToTable(
            "search for topics:",
            word ->
                context
                    .optionalAdmin()
                    .map(
                        admin -> {
                          executePlanButton.disable();
                          console.cleanup();
                          var partitions =
                              admin.partitions(
                                  admin.topicNames().stream()
                                      .filter(name -> word.isEmpty() || name.contains(word))
                                      .collect(Collectors.toSet()));
                          var topics =
                              partitions.stream().map(Partition::topic).collect(Collectors.toSet());
                          var clusterInfo = admin.clusterInfo();
                          console.append(
                              "start to generate optimized assignments for topics: " + topics);
                          var optionalPlan =
                              Balancer.builder()
                                  .planGenerator(new ShufflePlanGenerator(0, 3000))
                                  .clusterCost(new ReplicaLeaderCost())
                                  .limit(Duration.ofSeconds(10))
                                  .limit(10000)
                                  .clusterConstraint(
                                      (before, after) -> {
                                        var good = after.value() < before.value();
                                        if (good) {
                                          console.append(
                                              "find a assignment! The cost is reduced from : "
                                                  + before.value()
                                                  + " to: "
                                                  + after.value());
                                        }
                                        return good;
                                      })
                                  .build()
                                  .offer(clusterInfo, topics::contains, admin.brokerFolders());
                          optionalPlan.ifPresent(lastPlan::set);
                          return optionalPlan
                              .map(plan -> plan.proposal().rebalancePlan())
                              .map(
                                  allocation ->
                                      ClusterLogAllocation.findNonFulfilledAllocation(
                                              ClusterLogAllocation.of(clusterInfo), allocation)
                                          .stream()
                                          .map(
                                              tp ->
                                                  LinkedHashMap.of(
                                                      "topic",
                                                      tp.topic(),
                                                      "partition",
                                                      String.valueOf(tp.partition()),
                                                      "old assignments",
                                                      clusterInfo.replicas(tp).stream()
                                                          .map(
                                                              r ->
                                                                  r.nodeInfo().id()
                                                                      + ":"
                                                                      + r.dataFolder())
                                                          .collect(Collectors.joining(",")),
                                                      "new assignments",
                                                      allocation.logPlacements(tp).stream()
                                                          .map(
                                                              r ->
                                                                  r.nodeInfo().id()
                                                                      + ":"
                                                                      + r.dataFolder())
                                                          .collect(Collectors.joining(","))))
                                          .collect(Collectors.toList()))
                              .orElse(List.of());
                        })
                    .orElse(List.of()),
            (r, e) -> executePlanButton.enable());
    executePlanButton.setOnAction(
        ignored ->
            context
                .optionalAdmin()
                .ifPresent(
                    admin -> {
                      var plan = lastPlan.get();
                      if (plan == null) {
                        console.append("there is no optimized plan");
                        return;
                      }
                      executePlanButton.disable();
                      CompletableFuture.runAsync(
                              () -> {
                                var clusterInfo = admin.clusterInfo();
                                var allocation = plan.proposal().rebalancePlan();
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
                                tpAndReplicas.forEach(
                                    (tp, replicas) -> {
                                      admin
                                          .migrator()
                                          .partition(tp.topic(), tp.partition())
                                          .moveTo(
                                              replicas.stream()
                                                  .map(r -> r.nodeInfo().id())
                                                  .collect(Collectors.toList()));
                                    });

                                org.astraea.common.Utils.sleep(Duration.ofSeconds(5));

                                tpAndReplicas.forEach(
                                    (tp, replicas) -> {
                                      try {
                                        admin
                                            .migrator()
                                            .partition(tp.topic(), tp.partition())
                                            .moveTo(
                                                replicas.stream()
                                                    .collect(
                                                        Collectors.toMap(
                                                            r -> r.nodeInfo().id(),
                                                            Replica::dataFolder)));
                                        console.append("start to migrate " + tp);
                                      } catch (Exception e) {
                                        console.append(
                                            "failed to migrate "
                                                + tp
                                                + " due to "
                                                + e.getMessage());
                                      }
                                    });
                              })
                          .whenComplete(
                              (result, e) -> {
                                executePlanButton.enable();
                                console.append(e);
                              });
                    }));
    tab.setContent(Utils.vbox(search, executePlanButton, console));
    return tab;
  }
}
