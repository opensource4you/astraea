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

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.app.balancer.executor.StraightPlanExecutor;
import org.astraea.app.balancer.generator.ShufflePlanGenerator;
import org.astraea.app.scenario.Scenario;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BalancerTest extends RequireBrokerCluster {

  @Test
  void run() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topicName = Utils.randomString();
      var currentLeaders =
          (Supplier<Map<Integer, Long>>)
              () ->
                  admin.replicas().values().stream()
                      .map(x -> x.get(0))
                      .map(x -> x.nodeInfo().id())
                      .collect(Collectors.groupingBy(x -> x, Collectors.counting()));

      Scenario.build(0.5)
          .topicName(topicName)
          .numberOfPartitions(100)
          .numberOfReplicas((short) 1)
          .build()
          .apply(admin);
      var imbalanceFactor0 =
          Math.abs(
              currentLeaders.get().values().stream().mapToLong(x -> x).min().orElseThrow()
                  - currentLeaders.get().values().stream().mapToLong(x -> x).max().orElseThrow());

      for (int i = 0; i < 3; i++) {
        try {
          Balancer.builder()
              .usePlanGenerator(new ShufflePlanGenerator(1, 10), admin)
              .usePlanExecutor(new StraightPlanExecutor())
              .useClusterCost(new ReplicaLeaderCost())
              .searchLimit(1000)
              .build()
              .offer()
              .execute(admin);
        } catch (Exception e) {
          System.err.println(e.getMessage());
        }
      }

      var imbalanceFactor1 =
          Math.abs(
              currentLeaders.get().values().stream().mapToLong(x -> x).min().orElseThrow()
                  - currentLeaders.get().values().stream().mapToLong(x -> x).max().orElseThrow());
      Assertions.assertTrue(imbalanceFactor1 < imbalanceFactor0);
    }
  }
}
