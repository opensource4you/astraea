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
package org.astraea.common.cost;

import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.admin.Admin;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.it.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ClusterCostTest extends RequireSingleBrokerCluster {

  @Test
  void testMerge() {
    HasClusterCost cost0 = (c, b) -> () -> 0.2;
    HasClusterCost cost1 = (c, b) -> () -> 0.5;
    HasClusterCost cost2 = (c, b) -> () -> 0.8;
    var merged = HasClusterCost.of(Map.of(cost0, 1D, cost1, 2D, cost2, 2D));
    var result = merged.clusterCost(null, null).value();
    Assertions.assertEquals(2.8, Math.round(result * 100.0) / 100.0);
  }

  @Test
  void testFetcher() {
    // create topic partition to get metrics
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic("testFetcher").numberOfPartitions(2).run().toCompletableFuture().join();
    }
    var cost1 = new ReplicaSizeCost();
    var cost2 = new ReplicaLeaderCost();
    var mergeCost = HasClusterCost.of(Map.of(cost1, 1.0, cost2, 1.0));
    var metrics =
        mergeCost.fetcher().stream()
            .map(x -> x.fetch(MBeanClient.of(jmxServiceURL())))
            .collect(Collectors.toSet());
    Assertions.assertTrue(
        metrics.iterator().next().stream()
            .anyMatch(
                x ->
                    x.beanObject()
                        .properties()
                        .get("name")
                        .equals(LogMetrics.Log.SIZE.metricName())));
    Assertions.assertTrue(
        metrics.iterator().next().stream()
            .anyMatch(
                x ->
                    x.beanObject()
                        .properties()
                        .get("name")
                        .equals(ServerMetrics.ReplicaManager.LEADER_COUNT.metricName())));
  }
}
