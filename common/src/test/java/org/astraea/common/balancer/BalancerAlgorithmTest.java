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
package org.astraea.common.balancer;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.balancer.generator.ShufflePlanGenerator;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.ReplicaNumberCost;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

public class BalancerAlgorithmTest extends RequireBrokerCluster {

  @RepeatedTest(5)
  void test() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      IntStream.range(0, 5)
          .forEach(
              ignored ->
                  admin.creator().topic(Utils.randomString()).numberOfPartitions(10).create());
      admin
          .topicNames()
          .forEach(tp -> admin.migrator().topic(tp).moveTo(List.of(brokerIds().iterator().next())));
      Utils.sleep(Duration.ofSeconds(2));

      var planOfGreedy =
          Balancer.builder()
              .planGenerator(new ShufflePlanGenerator(0, 30))
              .clusterCost(List.of(new ReplicaNumberCost()))
              .limit(Duration.ofSeconds(5))
              .greedy(true)
              .build()
              .offer(admin.clusterInfo(), admin.brokerFolders())
              .get();

      var plan =
          Balancer.builder()
              .planGenerator(new ShufflePlanGenerator(0, 30))
              .clusterCost(List.of(new ReplicaNumberCost()))
              .limit(Duration.ofSeconds(5))
              .greedy(false)
              .build()
              .offer(admin.clusterInfo(), admin.brokerFolders())
              .get();

      Assertions.assertTrue(
          plan.clusterCosts.stream().mapToDouble(ClusterCost::value).sum()
              > planOfGreedy.clusterCosts.stream().mapToDouble(ClusterCost::value).sum());
    }
  }
}
