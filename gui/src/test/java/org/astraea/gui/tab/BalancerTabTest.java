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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.RebalancePlanProposal;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.cost.MoveCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.gui.Context;
import org.astraea.gui.pane.Input;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BalancerTabTest extends RequireBrokerCluster {

  @Test
  void testGenerator() {
    var topicName = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));
      var log = new AtomicReference<String>();
      var f =
          BalancerNode.generator(
              new Context(admin),
              Input.of(List.of("leader"), Map.of(BalancerNode.TOPIC_NAME_KEY, Optional.empty())),
              log::set);
      f.toCompletableFuture().join();
      Assertions.assertTrue(f.toCompletableFuture().isDone());
      Assertions.assertEquals(log.get(), "there is no better assignments");

      // migrate all replica to broker 0
      var tps =
          admin.topicPartitions(Set.of(topicName)).toCompletableFuture().join().stream()
              .map(tp -> Map.entry(tp, List.of(0)))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      admin.moveToBrokers(tps).toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      var s =
          BalancerNode.generator(
              new Context(admin),
              Input.of(List.of("leader"), Map.of(BalancerNode.TOPIC_NAME_KEY, Optional.empty())),
              log::set);
      s.toCompletableFuture().join();
      Assertions.assertTrue(s.toCompletableFuture().isDone());
      Assertions.assertTrue(
          log.get()
              .matches("find a better assignments. Total number of reassignments is" + "(.*)"));
    }
  }

  @Test
  void testResult() {
    var topic = Utils.randomString();
    var leaderSize = 100;
    var beforeReplicas =
        List.of(
            Replica.builder()
                .leader(true)
                .isPreferredLeader(false)
                .topic(topic)
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "aa", 0))
                .size(leaderSize)
                .path("/tmp/aaa")
                .build(),
            Replica.builder()
                .leader(false)
                .isPreferredLeader(true)
                .topic(topic)
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "aa", 0))
                .size(leaderSize)
                .path("/tmp/bbb")
                .build());
    var afterReplicas =
        List.of(
            Replica.builder()
                .leader(true)
                .isPreferredLeader(false)
                .topic(topic)
                .partition(0)
                .nodeInfo(NodeInfo.of(3, "aa", 0))
                .size(leaderSize)
                .path("/tmp/ddd")
                .build(),
            Replica.builder()
                .leader(false)
                .isPreferredLeader(true)
                .topic(topic)
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "aa", 0))
                .size(leaderSize)
                .path("/tmp/bbb")
                .build());
    var beforeClusterInfo = ClusterInfo.of(Set.of(), beforeReplicas);

    var results =
        BalancerNode.result(
            beforeClusterInfo,
            new Balancer.Plan(
                RebalancePlanProposal.builder()
                    .clusterLogAllocation(ClusterLogAllocation.of(ClusterInfo.of(afterReplicas)))
                    .build(),
                new ReplicaLeaderCost().clusterCost(beforeClusterInfo, ClusterBean.EMPTY),
                List.of(MoveCost.builder().build())));
    Assertions.assertEquals(results.size(), 1);
    Assertions.assertEquals(results.get(0).get("topic"), topic);
    Assertions.assertEquals(results.get(0).get("partition"), 0);
    Assertions.assertEquals(results.get(0).get("previous leader"), "0:/tmp/aaa");
    Assertions.assertEquals(results.get(0).get("new leader"), "3:/tmp/ddd");
    Assertions.assertEquals(results.get(0).get("previous follower"), "1:/tmp/bbb");
    Assertions.assertEquals(results.get(0).get("new follower"), "1:/tmp/bbb");
  }
}
