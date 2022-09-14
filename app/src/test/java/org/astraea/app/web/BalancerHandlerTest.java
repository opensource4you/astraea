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
package org.astraea.app.web;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.app.balancer.RebalancePlanProposal;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.ReplicaSizeCost;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BalancerHandlerTest extends RequireBrokerCluster {

  @Test
  void testReport() {
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin, new DegradeCost(), new ReplicaSizeCost());
      var report =
          Assertions.assertInstanceOf(
              BalancerHandler.Report.class,
              handler.get(Channel.ofQueries(Map.of(BalancerHandler.LIMIT_KEY, "3000"))));
      Assertions.assertEquals(3000, report.limit);
      Assertions.assertNotEquals(0, report.changes.size());
      Assertions.assertTrue(report.cost >= report.newCost);
      Assertions.assertEquals(
          handler.clusterCostFunction.getClass().getSimpleName(), report.function);
      // "before" should record size
      report.changes.stream()
          .flatMap(c -> c.before.stream())
          .forEach(p -> Assertions.assertNotEquals(0, p.size));
      // "after" should NOT record size
      report.changes.stream()
          .flatMap(c -> c.after.stream())
          .forEach(p -> Assertions.assertNull(p.size));
      Assertions.assertTrue(report.cost >= report.newCost);
      var sizeMigration =
          report.migrationCosts.stream().filter(x -> x.function.equals("size")).findFirst().get();
      Assertions.assertTrue(sizeMigration.totalCost >= 0);
      Assertions.assertTrue(sizeMigration.cost.size() > 0);
      Assertions.assertEquals(0, sizeMigration.cost.stream().mapToLong(x -> x.cost).sum());
    }
  }

  @Test
  void testTopic() {
    var topicNames = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin, new DegradeCost(), new ReplicaSizeCost());
      var report =
          Assertions.assertInstanceOf(
              BalancerHandler.Report.class,
              handler.get(
                  Channel.ofQueries(
                      Map.of(
                          BalancerHandler.LIMIT_KEY,
                          "30",
                          BalancerHandler.TOPICS_KEY,
                          topicNames.get(0)))));
      var actual =
          report.changes.stream().map(r -> r.topic).collect(Collectors.toUnmodifiableSet());
      Assertions.assertEquals(1, actual.size());
      Assertions.assertEquals(topicNames.get(0), actual.iterator().next());
      Assertions.assertTrue(report.cost >= report.newCost);
      var sizeMigration =
          report.migrationCosts.stream().filter(x -> x.function.equals("size")).findFirst().get();
      Assertions.assertTrue(sizeMigration.totalCost >= 0);
      Assertions.assertTrue(sizeMigration.cost.size() > 0);
      Assertions.assertEquals(0, sizeMigration.cost.stream().mapToLong(x -> x.cost).sum());
    }
  }

  /** The score will getter better after each call, pretend we find a better plan */
  private static class DegradeCost implements HasClusterCost {

    private double value0 = 1.0;

    @Override
    public synchronized ClusterCost clusterCost(
        ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
      value0 = value0 * 0.998;
      return () -> value0;
    }
  }

  @Test
  void testTopics() {
    var topicNames = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin, new DegradeCost(), new ReplicaSizeCost());
      var report =
          Assertions.assertInstanceOf(
              BalancerHandler.Report.class,
              handler.get(
                  Channel.ofQueries(
                      Map.of(
                          BalancerHandler.LIMIT_KEY,
                          "30",
                          BalancerHandler.TOPICS_KEY,
                          topicNames.get(0) + "," + topicNames.get(1)))));
      var actual =
          report.changes.stream().map(r -> r.topic).collect(Collectors.toUnmodifiableSet());
      Assertions.assertEquals(2, actual.size());
      Assertions.assertTrue(actual.contains(topicNames.get(0)));
      Assertions.assertTrue(actual.contains(topicNames.get(1)));
      Assertions.assertTrue(report.cost >= report.newCost);
      var sizeMigration =
          report.migrationCosts.stream().filter(x -> x.function.equals("size")).findFirst().get();
      Assertions.assertTrue(sizeMigration.totalCost >= 0);
      Assertions.assertTrue(sizeMigration.cost.size() > 0);
      Assertions.assertEquals(0, sizeMigration.cost.stream().mapToLong(x -> x.cost).sum());
    }
  }

  private static List<String> createAndProduceTopic(int topicCount) {
    try (var admin = Admin.of(bootstrapServers())) {
      var topics =
          IntStream.range(0, topicCount)
              .mapToObj(ignored -> Utils.randomString(10))
              .collect(Collectors.toUnmodifiableList());
      topics.forEach(
          topic ->
              admin
                  .creator()
                  .topic(topic)
                  .numberOfPartitions(3)
                  .numberOfReplicas((short) 1)
                  .create());
      Utils.sleep(Duration.ofSeconds(3));
      try (var producer = Producer.of(bootstrapServers())) {
        IntStream.range(0, 30)
            .forEach(
                index ->
                    topics.forEach(
                        topic ->
                            producer
                                .sender()
                                .topic(topic)
                                .key(String.valueOf(index).getBytes(StandardCharsets.UTF_8))
                                .run()));
      }
      return topics;
    }
  }

  @Test
  void testBestPlan() {
    var currentClusterInfo =
        ClusterInfo.of(
            List.of(NodeInfo.of(10, "host", 22), NodeInfo.of(11, "host", 22)),
            List.of(
                Replica.of(
                    "topic",
                    0,
                    NodeInfo.of(10, "host", 22),
                    0,
                    100,
                    true,
                    true,
                    false,
                    false,
                    true,
                    "/tmp/aa")));

    var clusterLogAllocation =
        ClusterLogAllocation.of(
            ClusterInfo.of(
                List.of(
                    Replica.of(
                        "topic",
                        0,
                        NodeInfo.of(11, "host", 22),
                        0,
                        100,
                        true,
                        true,
                        false,
                        false,
                        true,
                        "/tmp/aa"))));
    var proposal =
        RebalancePlanProposal.builder()
            .clusterLogAllocation(clusterLogAllocation)
            .index(100)
            .build();
    HasClusterCost clusterCostFunction =
        (clusterInfo, clusterBean) -> () -> clusterInfo == currentClusterInfo ? 100D : 10D;
    HasMoveCost moveCostFunction = (originClusterInfo, newClusterInfo, clusterBean) -> () -> 100;
    var best =
        BalancerHandler.bestPlan(
            Stream.of(proposal),
            currentClusterInfo,
            clusterCostFunction,
            clusterCost -> true,
            moveCostFunction,
            moveCost -> true);

    Assertions.assertNotEquals(Optional.empty(), best);
    Assertions.assertEquals(clusterLogAllocation, best.get().allocation);

    // test cluster cost predicate
    Assertions.assertEquals(
        Optional.empty(),
        BalancerHandler.bestPlan(
            Stream.of(proposal),
            currentClusterInfo,
            clusterCostFunction,
            clusterCost -> false,
            moveCostFunction,
            moveCost -> true));

    // test move cost predicate
    Assertions.assertEquals(
        Optional.empty(),
        BalancerHandler.bestPlan(
            Stream.of(proposal),
            currentClusterInfo,
            clusterCostFunction,
            clusterCost -> true,
            moveCostFunction,
            moveCost -> false));
  }
}
