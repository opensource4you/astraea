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
package org.astraea.gui.tab.health;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.cost.ReplicaLeaderSizeCost;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.gui.Context;
import org.astraea.gui.pane.Argument;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BalancerNodeTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testClusterCost() {
    Assertions.assertEquals(0, BalancerNode.clusterCosts(List.of()).size());
    Assertions.assertEquals(0, BalancerNode.clusterCosts(List.of(Utils.randomString())).size());

    var costs = BalancerNode.clusterCosts(List.of(BalancerNode.Cost.SIZE.name()));
    Assertions.assertEquals(1, costs.size());
    Assertions.assertInstanceOf(
        ReplicaLeaderSizeCost.class, costs.entrySet().iterator().next().getKey());
  }

  @Test
  void testGenerator() {
    var topicName = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
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
          BalancerNode.refresher(new Context(admin))
              .apply(
                  Argument.of(
                      List.of("leader"), Map.of(BalancerNode.TOPIC_NAME_KEY, Optional.empty())),
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
          BalancerNode.refresher(new Context(admin))
              .apply(
                  Argument.of(
                      List.of("leader"), Map.of(BalancerNode.TOPIC_NAME_KEY, Optional.empty())),
                  log::set);
      s.toCompletableFuture().join();
      Assertions.assertTrue(s.toCompletableFuture().isDone());
      Assertions.assertTrue(log.get().contains("find a plan"));
    }
  }

  @Test
  void testResult() {
    var topic = Utils.randomString();
    var leaderSize = 100;
    var allNodes = List.of(Broker.of(0, "aa", 0), Broker.of(1, "aa", 0), Broker.of(3, "aa", 0));
    var beforeReplicas =
        List.of(
            Replica.builder()
                .isLeader(true)
                .isPreferredLeader(false)
                .topic(topic)
                .partition(0)
                .brokerId(allNodes.get(0).id())
                .size(leaderSize)
                .path("/tmp/aaa")
                .build(),
            Replica.builder()
                .isLeader(false)
                .isPreferredLeader(true)
                .topic(topic)
                .partition(0)
                .brokerId(allNodes.get(1).id())
                .size(leaderSize)
                .path("/tmp/bbb")
                .build());
    var afterReplicas =
        List.of(
            Replica.builder()
                .isLeader(true)
                .isPreferredLeader(false)
                .topic(topic)
                .partition(0)
                .brokerId(allNodes.get(2).id())
                .size(leaderSize)
                .path("/tmp/ddd")
                .build(),
            Replica.builder()
                .isLeader(false)
                .isPreferredLeader(true)
                .topic(topic)
                .partition(0)
                .brokerId(allNodes.get(1).id())
                .size(leaderSize)
                .path("/tmp/bbb")
                .build());
    var beforeClusterInfo = ClusterInfo.of("fake", List.of(), Map.of(), beforeReplicas);

    var results =
        BalancerNode.assignmentResult(
            new Balancer.Plan(
                ClusterBean.EMPTY,
                beforeClusterInfo,
                () -> 1.0D,
                ClusterInfo.of("fake", allNodes, Map.of(), afterReplicas),
                () -> 1.0D));
    Assertions.assertEquals(results.size(), 1);
    Assertions.assertEquals(results.get(0).get("topic"), topic);
    Assertions.assertEquals(results.get(0).get("partition"), 0);
    Assertions.assertEquals(results.get(0).get("previous leader"), "0:/tmp/aaa");
    Assertions.assertEquals(results.get(0).get("new leader"), "3:/tmp/ddd");
    Assertions.assertEquals(results.get(0).get("previous follower"), "1:/tmp/bbb");
    Assertions.assertEquals(results.get(0).get("new follower"), "1:/tmp/bbb");
  }
}
