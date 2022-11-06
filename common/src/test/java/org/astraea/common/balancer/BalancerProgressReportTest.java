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
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.balancer.reports.BalancerProgressReport;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.Configuration;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BalancerProgressReportTest extends RequireBrokerCluster {

  @Test
  void testMerge() {
    // TODO: fill in the test
  }

  @Test
  void testCost() {
    // TODO: fill in the test
  }

  @Test
  void testFunctionReturnVoid() {
    // TODO: fill in the test
  }

  @ParameterizedTest
  @ValueSource(classes = {SingleStepBalancer.class, GreedyBalancer.class})
  void testWithBalancer(Class<? extends Balancer> balancer) {
    try (Admin admin = Admin.of(bootstrapServers())) {
      int iteration = 1000;
      var invoked = new LongAdder();
      var topic = Utils.randomString();
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(10)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));

      var reporter =
          new BalancerProgressReport() {
            @Override
            public void iteration(long time, double clusterCost) {
              System.out.println(time + ": " + clusterCost);
              invoked.increment();
            }
          };
      var info = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join();
      Balancer.create(
              balancer,
              AlgorithmConfig.builder()
                  .clusterCost(new DecreasingCost(null))
                  .config("iteration", Integer.toString(iteration))
                  .build())
          .offer(info, logFolders(), reporter);

      Assertions.assertTrue(invoked.sum() > 0, "The Reporter has been invoked");
    }
  }

  public static class DecreasingCost implements HasClusterCost {

    private ClusterInfo<Replica> original;

    public DecreasingCost(Configuration configuration) {}

    private double value0 = 1.0;

    @Override
    public synchronized ClusterCost clusterCost(
        ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
      if (original == null) original = clusterInfo;
      if (ClusterInfo.findNonFulfilledAllocation(original, clusterInfo).isEmpty()) return () -> 1;
      double theCost = value0;
      value0 = value0 * 0.998;
      return () -> theCost;
    }
  }
}
