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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.generator.RebalancePlanGenerator;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;
import org.astraea.common.cost.ReplicaSizeCost;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BalancerHandlerTest extends RequireBrokerCluster {

  @Test
  void testReport() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler =
          new BalancerHandler(admin, MultiplicationCost.decreasing(), new ReplicaSizeCost());
      var report =
          Assertions.assertInstanceOf(
              BalancerHandler.Report.class,
              handler.get(Channel.ofQueries(Map.of(BalancerHandler.LOOP_KEY, "3000"))));
      Assertions.assertNotNull(report.id);
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
      var handler =
          new BalancerHandler(admin, MultiplicationCost.decreasing(), new ReplicaSizeCost());
      var report =
          Assertions.assertInstanceOf(
              BalancerHandler.Report.class,
              handler.get(
                  Channel.ofQueries(
                      Map.of(
                          BalancerHandler.LOOP_KEY,
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
  private static class MultiplicationCost implements HasClusterCost {

    private double value0 = 1.0;
    private final double mul;

    static MultiplicationCost decreasing() {
      return new MultiplicationCost(0.998);
    }

    static MultiplicationCost increasing() {
      return new MultiplicationCost(1.02);
    }

    private MultiplicationCost(double mul) {
      this.mul = mul;
    }

    @Override
    public synchronized ClusterCost clusterCost(
        ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
      double theCost = value0;
      value0 = value0 * mul;
      return () -> theCost;
    }
  }

  @Test
  void testTopics() {
    var topicNames = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler =
          new BalancerHandler(admin, MultiplicationCost.decreasing(), new ReplicaSizeCost());
      var report =
          Assertions.assertInstanceOf(
              BalancerHandler.Report.class,
              handler.get(
                  Channel.ofQueries(
                      Map.of(
                          BalancerHandler.LOOP_KEY,
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
    try (var admin = Admin.of(bootstrapServers())) {
      var currentClusterInfo =
          ClusterInfo.of(
              Set.of(NodeInfo.of(10, "host", 22), NodeInfo.of(11, "host", 22)),
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
      HasClusterCost clusterCostFunction =
          (clusterInfo, clusterBean) -> () -> clusterInfo == currentClusterInfo ? 100D : 10D;
      HasMoveCost moveCostFunction =
          (originClusterInfo, newClusterInfo, clusterBean) ->
              MoveCost.builder().totalCost(100).build();

      var balancerHandler =
          new BalancerHandler(admin, MultiplicationCost.decreasing(), new ReplicaSizeCost());
      var Best =
          Balancer.builder()
              .planGenerator(RebalancePlanGenerator.random(30))
              .clusterCost(clusterCostFunction)
              .clusterConstraint((before, after) -> after.value() <= before.value())
              .moveCost(moveCostFunction)
              .movementConstraint(moveCost -> true)
              .build()
              .offer(admin.clusterInfo(), ignore -> true, admin.brokerFolders());

      Assertions.assertNotEquals(Optional.empty(), Best);

      // test loop limit
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              Balancer.builder()
                  .planGenerator(RebalancePlanGenerator.random(30))
                  .clusterCost(clusterCostFunction)
                  .clusterConstraint((before, after) -> true)
                  .moveCost(moveCostFunction)
                  .movementConstraint(moveCost -> true)
                  .limit(0)
                  .build()
                  .offer(admin.clusterInfo(), ignore -> true, admin.brokerFolders()));

      // test cluster cost predicate
      Assertions.assertEquals(
          Optional.empty(),
          Balancer.builder()
              .planGenerator(RebalancePlanGenerator.random(30))
              .clusterCost(clusterCostFunction)
              .clusterConstraint((before, after) -> false)
              .moveCost(moveCostFunction)
              .movementConstraint(moveCost -> true)
              .build()
              .offer(admin.clusterInfo(), ignore -> true, admin.brokerFolders()));

      // test move cost predicate
      Assertions.assertEquals(
          Optional.empty(),
          Balancer.builder()
              .planGenerator(RebalancePlanGenerator.random(30))
              .clusterCost(clusterCostFunction)
              .clusterConstraint((before, after) -> true)
              .moveCost(moveCostFunction)
              .movementConstraint(moveCost -> false)
              .build()
              .offer(admin.clusterInfo(), ignore -> true, admin.brokerFolders()));
    }
  }

  @Test
  void testNoReport() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      Utils.sleep(Duration.ofSeconds(1));
      var handler =
          new BalancerHandler(admin, MultiplicationCost.increasing(), new ReplicaSizeCost());
      var report =
          Assertions.assertInstanceOf(
              BalancerHandler.Report.class,
              handler.get(Channel.ofQueries(Map.of(BalancerHandler.LOOP_KEY, "10"))));

      Assertions.assertTrue(report.changes.isEmpty());
      Assertions.assertNull(report.id);
    }
  }
}
