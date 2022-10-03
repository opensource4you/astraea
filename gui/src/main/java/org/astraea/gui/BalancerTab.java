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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.generator.ShufflePlanGenerator;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.cost.ReplicaNumberCost;

public class BalancerTab {

  public static Tab of(Context context) {
    var tab = new Tab("balance topic");
    BiFunction<String, Console, SearchResult<Balancer.Plan>> planGenerator =
        (word, console) -> {
          var optionalAdmin = context.optionalAdmin();
          if (optionalAdmin.isEmpty()) return SearchResult.empty();
          var admin = optionalAdmin.get();
          var partitions =
              admin.partitions(
                  admin.topicNames().stream()
                      .filter(name -> word.isEmpty() || name.contains(word))
                      .collect(Collectors.toSet()));
          var topics = partitions.stream().map(Partition::topic).collect(Collectors.toSet());
          var clusterInfo = admin.clusterInfo();
          console.append("start to generate optimized assignments for topics: " + topics);
          System.out.println(
              "nodes: "
                  + clusterInfo.nodes().stream().map(NodeInfo::id).collect(Collectors.toList()));
          System.out.println("admin.brokerFolders(): " + admin.brokerFolders());
          var optionalPlan =
              Balancer.builder()
                  .planGenerator(new ShufflePlanGenerator(0, 30))
                  .clusterCost(new ReplicaNumberCost())
                  .limit(Duration.ofSeconds(10))
                  .limit(10000)
                  .greedy(true)
                  .build()
                  .offer(clusterInfo, topics::contains, admin.brokerFolders());
          if (optionalPlan.isEmpty()) return SearchResult.empty();
          var plan = optionalPlan.get();
          var allocation = plan.proposal().rebalancePlan();
          var items =
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
                                  .map(r -> r.nodeInfo().id() + ":" + r.dataFolder())
                                  .collect(Collectors.joining(",")),
                              "new assignments",
                              allocation.logPlacements(tp).stream()
                                  .map(r -> r.nodeInfo().id() + ":" + r.dataFolder())
                                  .collect(Collectors.joining(","))))
                  .collect(Collectors.toList());
          return SearchResult.of(items, plan);
        };

    BiConsumer<SearchResult<Balancer.Plan>, Console> planExecutor =
        (result, console) -> {
          var optionalAdmin = context.optionalAdmin();
          if (optionalAdmin.isEmpty()) return;
          var admin = optionalAdmin.get();
          var clusterInfo = admin.clusterInfo();
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
                                  .sorted(Comparator.comparing(Replica::isPreferredLeader))
                                  .collect(Collectors.toList())));
          tpAndReplicas.forEach(
              (tp, replicas) -> {
                admin
                    .migrator()
                    .partition(tp.topic(), tp.partition())
                    .moveTo(
                        replicas.stream().map(r -> r.nodeInfo().id()).collect(Collectors.toList()));
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
                                  Collectors.toMap(r -> r.nodeInfo().id(), Replica::dataFolder)));
                  console.append("start to migrate " + tp);
                } catch (Exception e) {
                  console.append("failed to migrate " + tp + " due to " + e.getMessage());
                }
              });
        };

    tab.setContent(
        Utils.searchToTable("topic name (space means all topics):", planGenerator, planExecutor));
    return tab;
  }
}
