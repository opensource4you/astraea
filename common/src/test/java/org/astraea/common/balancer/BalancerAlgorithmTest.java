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
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.cost.ReplicaNumberCost;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

public class BalancerAlgorithmTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @RepeatedTest(5)
  void test() {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(10)
          .run()
          .toCompletableFuture()
          .join();
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(10)
          .run()
          .toCompletableFuture()
          .join();
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(10)
          .run()
          .toCompletableFuture()
          .join();
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(10)
          .run()
          .toCompletableFuture()
          .join();
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(10)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      admin
          .moveToBrokers(
              admin
                  .topicPartitions(admin.topicNames(false).toCompletableFuture().join())
                  .toCompletableFuture()
                  .join()
                  .stream()
                  .collect(
                      Collectors.toMap(
                          tp -> tp,
                          tp -> List.of(SERVICE.dataFolders().keySet().iterator().next()))))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      var planOfGreedy =
          Utils.construct(GreedyBalancer.class, Configuration.EMPTY)
              .offer(
                  AlgorithmConfig.builder()
                      .clusterInfo(
                          admin
                              .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                              .toCompletableFuture()
                              .join())
                      .clusterBean(ClusterBean.EMPTY)
                      .timeout(Duration.ofSeconds(5))
                      .clusterCost(new ReplicaNumberCost())
                      .build())
              .solution()
              .get();

      var plan =
          Utils.construct(SingleStepBalancer.class, Configuration.EMPTY)
              .offer(
                  AlgorithmConfig.builder()
                      .clusterInfo(
                          admin
                              .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                              .toCompletableFuture()
                              .join())
                      .clusterBean(ClusterBean.EMPTY)
                      .timeout(Duration.ofSeconds(5))
                      .clusterCost(new ReplicaNumberCost())
                      .build())
              .solution()
              .get();

      Assertions.assertTrue(
          plan.proposalClusterCost.value() > planOfGreedy.proposalClusterCost.value());
    }
  }
}
