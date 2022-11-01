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
package org.astraea.app.balancer;

import com.beust.jcommander.Parameter;
import java.time.Duration;
import java.util.Set;
import java.util.function.Predicate;
import org.astraea.common.admin.Admin;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
import org.astraea.common.cost.ReplicaLeaderCost;

/**
 * A simple demo for Balancer, it does the following:
 *
 * <ol>
 *   <li>Generate a random plan for the target cluster.
 *   <li>Print the information of the generated plan.
 *   <li>Execute the generated plan against the target cluster.
 * </ol>
 */
public class BalanceProcessDemo {
  public static void main(String[] args) {
    var argument = org.astraea.common.argument.Argument.parse(new Argument(), args);
    try (var admin = Admin.of(argument.configs())) {
      var clusterInfo =
          admin
              .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
              .toCompletableFuture()
              .join();
      var brokerFolders = admin.brokerFolders().toCompletableFuture().join();
      Predicate<String> filter = topic -> !argument.ignoredTopics.contains(topic);
      var plan =
          Balancer.Official.SingleStep.create(
                  AlgorithmConfig.builder()
                      .clusterCost(new ReplicaLeaderCost())
                      .topicFilter(filter)
                      .limit(1000)
                      .build())
              .offer(clusterInfo, brokerFolders);
      plan.ifPresent(
          p ->
              new StraightPlanExecutor()
                  .execute(admin, p.proposal().rebalancePlan(), Duration.ofHours(1))
                  .toCompletableFuture()
                  .join());
    }
  }

  public static class Argument extends org.astraea.common.argument.Argument {
    @Parameter(names = {"--ignored.topics"})
    public Set<String> ignoredTopics =
        Set.of("__consumer_offsets", "__transaction_state", "__cluster_metadata");
  }
}
