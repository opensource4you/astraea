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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class BalancerConsoleTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testBalancerConsole() {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      // create some topics
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(10)
          .numberOfReplicas((short) 2)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofMillis(500));

      // launch rebalance plan generation
      var console = BalancerConsole.create(admin, (x) -> Optional.empty());
      var generation =
          console
              .launchRebalancePlanGeneration()
              .setTaskId("THE_TASK")
              .setBalancer(new SingleStepBalancer(Configuration.EMPTY))
              .setGenerationTimeout(Duration.ofSeconds(1))
              .setAlgorithmConfig(
                  AlgorithmConfig.builder().clusterCost(new DecreasingCost()).build())
              .generate();
      Assertions.assertEquals(
          BalancerConsole.TaskPhase.Searching, console.taskPhase("THE_TASK").orElseThrow());
      Assertions.assertFalse(generation.toCompletableFuture().isDone());
      generation.toCompletableFuture().join();
      Assertions.assertEquals(
          BalancerConsole.TaskPhase.Searched, console.taskPhase("THE_TASK").orElseThrow());
      Assertions.assertTrue(generation.toCompletableFuture().isDone());

      // this task is there
      Assertions.assertTrue(console.taskPhase("THE_TASK").isPresent());
      Assertions.assertEquals(Set.of("THE_TASK"), console.tasks());

      // launch rebalance plan execution
      var execution =
          console
              .launchRebalancePlanExecution()
              .setExecutionTimeout(Duration.ofSeconds(1))
              .setExecutor(new TimeoutExecutor())
              .execute("THE_TASK");
      Assertions.assertEquals(
          BalancerConsole.TaskPhase.Executing, console.taskPhase("THE_TASK").orElseThrow());
      Assertions.assertTrue(generation.toCompletableFuture().isDone());
      Assertions.assertFalse(execution.toCompletableFuture().isDone());
      execution.toCompletableFuture().join();
      Assertions.assertEquals(
          BalancerConsole.TaskPhase.Executed, console.taskPhase("THE_TASK").orElseThrow());
      Assertions.assertTrue(generation.toCompletableFuture().isDone());
      Assertions.assertTrue(execution.toCompletableFuture().isDone());

      Assertions.assertDoesNotThrow(console::close);
    }
  }

  @Test
  void testTasks() {
    try (Admin admin = Admin.of(SERVICE.bootstrapServers())) {
      // create some topics
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(10)
          .numberOfReplicas((short) 2)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofMillis(500));

      // launch rebalance plan generation
      var console = BalancerConsole.create(admin, (x) -> Optional.empty());
      console
          .launchRebalancePlanGeneration()
          .setTaskId("TASK_1")
          .setBalancer(new SingleStepBalancer(Configuration.EMPTY))
          .setGenerationTimeout(Duration.ofMillis(100))
          .checkNoOngoingMigration(false)
          .setAlgorithmConfig(AlgorithmConfig.builder().clusterCost(new DecreasingCost()).build())
          .generate();
      console
          .launchRebalancePlanGeneration()
          .setTaskId("TASK_2")
          .setBalancer(new SingleStepBalancer(Configuration.EMPTY))
          .setGenerationTimeout(Duration.ofMillis(100))
          .checkNoOngoingMigration(false)
          .setAlgorithmConfig(AlgorithmConfig.builder().clusterCost(new DecreasingCost()).build())
          .generate();
      console
          .launchRebalancePlanGeneration()
          .setTaskId("TASK_3")
          .setBalancer(new SingleStepBalancer(Configuration.EMPTY))
          .setGenerationTimeout(Duration.ofMillis(100))
          .checkNoOngoingMigration(false)
          .setAlgorithmConfig(AlgorithmConfig.builder().clusterCost(new DecreasingCost()).build())
          .generate();
      Assertions.assertEquals(Set.of("TASK_1", "TASK_2", "TASK_3"), console.tasks());
    }
  }

  @Test
  void testFreshJmxAddress() {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      var noJmx = (BalancerConsoleImpl) BalancerConsole.create(admin, (id) -> Optional.empty());
      var withJmx = (BalancerConsoleImpl) BalancerConsole.create(admin, (id) -> Optional.of(5566));
      var partialJmx =
          (BalancerConsoleImpl)
              BalancerConsole.create(admin, (id) -> Optional.ofNullable(id != 0 ? 1000 : null));

      Assertions.assertEquals(0, noJmx.freshJmxAddresses().size());
      Assertions.assertEquals(3, withJmx.freshJmxAddresses().size());
      Assertions.assertThrows(IllegalArgumentException.class, partialJmx::freshJmxAddresses);
    }
  }

  @Test
  void testCheckPlanConsistency() {
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var console = BalancerConsole.create(admin, (x) -> Optional.empty())) {
      var topic = Utils.randomString();
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(10)
          .numberOfReplicas((short) 2)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofMillis(500));

      var generation =
          console
              .launchRebalancePlanGeneration()
              .setTaskId("THE_TASK")
              .setBalancer(new SingleStepBalancer(Configuration.EMPTY))
              .setGenerationTimeout(Duration.ofSeconds(1))
              .setAlgorithmConfig(
                  AlgorithmConfig.builder().clusterCost(new DecreasingCost()).build())
              .generate();

      // change the cluster state via moving things
      admin
          .moveToBrokers(
              Map.of(TopicPartition.of(topic, 0), List.copyOf(SERVICE.dataFolders().keySet())))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofMillis(500));
      generation.toCompletableFuture().join();

      Assertions.assertThrows(
          IllegalStateException.class,
          () ->
              console
                  .launchRebalancePlanExecution()
                  .setExecutor(new StraightPlanExecutor())
                  .checkPlanConsistency(true)
                  .execute("THE_TASK"),
          "Cluster state has been changed");
      Assertions.assertDoesNotThrow(
              () ->
                  console
                      .launchRebalancePlanExecution()
                      .setExecutor(new NoOpExecutor())
                      .checkPlanConsistency(false)
                      .execute("THE_TASK"),
              "Cluster state has been changed, but no check perform")
          .toCompletableFuture()
          .join();
    }
  }

  @Test
  void testCheckNoOngoingMigration() {
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var spy = Mockito.spy(admin);
        var console = BalancerConsole.create(spy, (x) -> Optional.empty())) {
      var topic = Utils.randomString();
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(10)
          .numberOfReplicas((short) 2)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofMillis(500));

      Assertions.assertDoesNotThrow(
          () ->
              console
                  .launchRebalancePlanGeneration()
                  .setTaskId(Utils.randomString())
                  .setBalancer(new SingleStepBalancer(Configuration.EMPTY))
                  .setAlgorithmConfig(
                      AlgorithmConfig.builder().clusterCost(new DecreasingCost()).build())
                  .setGenerationTimeout(Duration.ofMillis(100))
                  .checkNoOngoingMigration(true)
                  .generate(),
          "No change occurred");

      var cluster =
          admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();
      Stream.<Function<Replica, Replica>>of(
              (r) -> Replica.builder(r).isAdding(true).build(),
              (r) -> Replica.builder(r).isRemoving(true).build(),
              (r) -> Replica.builder(r).isFuture(true).build())
          .forEach(
              mapper -> {
                Mockito.doReturn(
                        CompletableFuture.completedFuture(
                            ClusterInfo.of(
                                cluster.clusterId(),
                                cluster.nodes(),
                                cluster
                                    .replicaStream()
                                    .map(mapper)
                                    .collect(Collectors.toUnmodifiableList()))))
                    .when(spy)
                    .clusterInfo(Mockito.anySet());
                Assertions.assertThrows(
                    IllegalStateException.class,
                    () ->
                        console
                            .launchRebalancePlanGeneration()
                            .setTaskId(Utils.randomString())
                            .setBalancer(new SingleStepBalancer(Configuration.EMPTY))
                            .setAlgorithmConfig(
                                AlgorithmConfig.builder().clusterCost(new DecreasingCost()).build())
                            .setGenerationTimeout(Duration.ofMillis(100))
                            .checkNoOngoingMigration(true)
                            .generate(),
                    "Some Adding/Removing/Future replica here");
                Assertions.assertDoesNotThrow(
                    () ->
                        console
                            .launchRebalancePlanGeneration()
                            .setTaskId(Utils.randomString())
                            .setBalancer(new SingleStepBalancer(Configuration.EMPTY))
                            .setAlgorithmConfig(
                                AlgorithmConfig.builder().clusterCost(new DecreasingCost()).build())
                            .setGenerationTimeout(Duration.ofMillis(100))
                            .checkNoOngoingMigration(false)
                            .generate(),
                    "Some Adding/Removing/Future replica here, but no check performed");
              });
    }
  }

  @Test
  void testExecutionAheadOfGeneration() {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      // create some topics
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(10)
          .numberOfReplicas((short) 2)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofMillis(500));

      // launch rebalance plan generation
      var console = BalancerConsole.create(admin, t -> Optional.empty());
      var generation =
          console
              .launchRebalancePlanGeneration()
              .setTaskId("THE_TASK")
              .setBalancer(new SingleStepBalancer(Configuration.EMPTY))
              .setGenerationTimeout(Duration.ofSeconds(1))
              .setAlgorithmConfig(
                  AlgorithmConfig.builder().clusterCost(new DecreasingCost()).build())
              .generate();
      var execution =
          console
              .launchRebalancePlanExecution()
              .setExecutionTimeout(Duration.ofSeconds(1))
              .setExecutor(new TimeoutExecutor())
              .execute("THE_TASK");

      // generation
      Assertions.assertEquals(
          BalancerConsole.TaskPhase.Searching, console.taskPhase("THE_TASK").orElseThrow());

      // wait until generation done
      generation.toCompletableFuture().join();

      // execution
      Assertions.assertEquals(
          BalancerConsole.TaskPhase.Executing, console.taskPhase("THE_TASK").orElseThrow());

      // wait until execution done
      execution.toCompletableFuture().join();

      // execution done
      Assertions.assertEquals(
          BalancerConsole.TaskPhase.Executed, console.taskPhase("THE_TASK").orElseThrow());
    }
  }

  public static class DecreasingCost implements HasClusterCost {

    private ClusterInfo original;
    private double value0 = 1.0;

    public DecreasingCost() {}

    @Override
    public synchronized ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
      if (original == null) original = clusterInfo;
      if (ClusterInfo.findNonFulfilledAllocation(original, clusterInfo).isEmpty()) return () -> 1;
      double theCost = value0;
      value0 = value0 * 0.998;
      return () -> theCost;
    }
  }

  public static class NoOpExecutor implements RebalancePlanExecutor {

    @Override
    public CompletionStage<Void> run(Admin admin, ClusterInfo targetAllocation, Duration timeout) {
      return CompletableFuture.completedFuture(null);
    }
  }

  public static class TimeoutExecutor implements RebalancePlanExecutor {

    @Override
    public CompletionStage<Void> run(Admin admin, ClusterInfo targetAllocation, Duration timeout) {
      return CompletableFuture.runAsync(() -> Utils.sleep(timeout));
    }
  }
}
