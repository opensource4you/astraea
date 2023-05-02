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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class BalancerTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @ParameterizedTest
  @ValueSource(classes = {SingleStepBalancer.class, GreedyBalancer.class})
  void testLeaderCountRebalance(Class<? extends Balancer> theClass) {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      var topicName = Utils.randomString();
      var currentLeaders =
          (Supplier<Map<Integer, Long>>)
              () -> {
                var clusterInfo =
                    admin
                        .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                        .toCompletableFuture()
                        .join();
                return clusterInfo.nodes().stream()
                    .collect(
                        Collectors.toMap(
                            NodeInfo::id,
                            n ->
                                clusterInfo
                                    .replicaStream(n.id())
                                    .filter(Replica::isLeader)
                                    .count()));
              };
      var currentImbalanceFactor =
          (Supplier<Long>)
              () ->
                  currentLeaders.get().values().stream().mapToLong(x -> x).max().orElseThrow()
                      - currentLeaders.get().values().stream()
                          .mapToLong(x -> x)
                          .min()
                          .orElseThrow();

      admin.creator().topic(topicName).numberOfPartitions(100).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      admin
          .moveToBrokers(
              IntStream.range(0, 100)
                  .mapToObj(i -> TopicPartition.of(topicName, i))
                  .collect(
                      Collectors.toMap(
                          Function.identity(),
                          ignored -> List.of(SERVICE.dataFolders().keySet().iterator().next()))))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      var imbalanceFactor0 = currentImbalanceFactor.get();
      Assertions.assertNotEquals(
          0, imbalanceFactor0, "This cluster is completely balanced in terms of leader count");

      var plan =
          Utils.construct(theClass, Configuration.EMPTY)
              .offer(
                  AlgorithmConfig.builder()
                      .clusterInfo(
                          admin
                              .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                              .toCompletableFuture()
                              .join())
                      .clusterBean(ClusterBean.EMPTY)
                      .timeout(Duration.ofSeconds(10))
                      .clusterCost(new ReplicaLeaderCost())
                      .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, "(" + topicName + ")")
                      .build())
              .orElseThrow();
      new StraightPlanExecutor(Configuration.EMPTY)
          .run(admin, plan.proposal(), Duration.ofSeconds(10))
          .toCompletableFuture()
          .join();

      var imbalanceFactor1 = currentImbalanceFactor.get();
      Assertions.assertTrue(
          imbalanceFactor1 < imbalanceFactor0,
          "Leader count should be closer, original: "
              + imbalanceFactor0
              + ". now: "
              + imbalanceFactor1);
    }
  }

  @ParameterizedTest
  @ValueSource(classes = {SingleStepBalancer.class, GreedyBalancer.class})
  void testFilter(Class<? extends Balancer> theClass) {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      var theTopic = Utils.randomString();
      var topic1 = Utils.randomString();
      var topic2 = Utils.randomString();
      var topic3 = Utils.randomString();
      admin.creator().topic(theTopic).numberOfPartitions(10).run().toCompletableFuture().join();
      admin.creator().topic(topic1).numberOfPartitions(10).run().toCompletableFuture().join();
      admin.creator().topic(topic2).numberOfPartitions(10).run().toCompletableFuture().join();
      admin.creator().topic(topic3).numberOfPartitions(10).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));

      var randomScore =
          new HasClusterCost() {
            @Override
            public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
              return () -> ThreadLocalRandom.current().nextDouble();
            }
          };

      var clusterInfo =
          admin
              .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
              .toCompletableFuture()
              .join();
      var newAllocation =
          Utils.construct(theClass, Configuration.of(Map.of("iteration", "500")))
              .offer(
                  AlgorithmConfig.builder()
                      .clusterInfo(clusterInfo)
                      .clusterBean(ClusterBean.EMPTY)
                      .timeout(Duration.ofSeconds(3))
                      .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, "(" + theTopic + ")")
                      .clusterCost(randomScore)
                      .build())
              .get()
              .proposal();

      var currentCluster =
          admin
              .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
              .toCompletableFuture()
              .join();

      Assertions.assertEquals(
          currentCluster.replicas(topic1).stream()
              .map(Replica::topicPartitionReplica)
              .collect(Collectors.toSet()),
          newAllocation.replicas(topic1).stream()
              .map(Replica::topicPartitionReplica)
              .collect(Collectors.toSet()),
          "With filter, only specific topic has been balanced");
      Assertions.assertEquals(
          currentCluster.replicas(topic2).stream()
              .map(Replica::topicPartitionReplica)
              .collect(Collectors.toSet()),
          newAllocation.replicas(topic2).stream()
              .map(Replica::topicPartitionReplica)
              .collect(Collectors.toSet()),
          "With filter, only specific topic has been balanced");
      Assertions.assertEquals(
          currentCluster.replicas(topic3).stream()
              .map(Replica::topicPartitionReplica)
              .collect(Collectors.toSet()),
          newAllocation.replicas(topic3).stream()
              .map(Replica::topicPartitionReplica)
              .collect(Collectors.toSet()),
          "With filter, only specific topic has been balanced");
    }
  }

  @ParameterizedTest
  @ValueSource(classes = {SingleStepBalancer.class, GreedyBalancer.class})
  void testExecutionTime(Class<? extends Balancer> theClass) {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      var theTopic = Utils.randomString();
      var topic1 = Utils.randomString();
      var topic2 = Utils.randomString();
      var topic3 = Utils.randomString();
      admin.creator().topic(theTopic).numberOfPartitions(10).run().toCompletableFuture().join();
      admin.creator().topic(topic1).numberOfPartitions(10).run().toCompletableFuture().join();
      admin.creator().topic(topic2).numberOfPartitions(10).run().toCompletableFuture().join();
      admin.creator().topic(topic3).numberOfPartitions(10).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));
      var future =
          CompletableFuture.supplyAsync(
              () ->
                  Utils.construct(theClass, Configuration.EMPTY)
                      .offer(
                          AlgorithmConfig.builder()
                              .clusterInfo(
                                  admin
                                      .clusterInfo(
                                          admin.topicNames(false).toCompletableFuture().join())
                                      .toCompletableFuture()
                                      .join())
                              .clusterBean(ClusterBean.EMPTY)
                              .timeout(Duration.ofSeconds(3))
                              .clusterCost((clusterInfo, bean) -> Math::random)
                              .build())
                      .get()
                      .proposal());
      Utils.sleep(Duration.ofMillis(1000));
      Assertions.assertFalse(future.isDone());
      Utils.sleep(Duration.ofMillis(2500));
      Assertions.assertTrue(future.isDone());
      Assertions.assertFalse(future.isCompletedExceptionally());
      Assertions.assertNotNull(future.join());
    }
  }

  @ParameterizedTest
  @ValueSource(classes = {SingleStepBalancer.class, GreedyBalancer.class})
  void testWithMetrics(Class<? extends Balancer> theClass) {
    var counter = new AtomicLong();
    Supplier<ClusterBean> metricSource =
        () -> {
          // increment the counter as the bean updated
          final var value = counter.getAndIncrement();
          final var mock = Mockito.mock(HasBeanObject.class);
          Mockito.when(mock.createdTimestamp()).thenReturn(value);
          Mockito.when(mock.beanObject()).thenReturn(new BeanObject("", Map.of(), Map.of()));
          return ClusterBean.of(Map.of(0, List.of(mock)));
        };
    Consumer<Long> test =
        (expected) -> {
          var called = new AtomicBoolean();
          var theCostFunction =
              new HasClusterCost() {
                @Override
                public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
                  Assertions.assertEquals(1, clusterBean.all().get(0).size());
                  Assertions.assertEquals(
                      expected,
                      clusterBean.all().get(0).stream()
                          .findFirst()
                          .orElseThrow()
                          .createdTimestamp(),
                      "The metric counter increased");
                  called.set(true);
                  return () -> 0;
                }
              };
          Utils.construct(theClass, Configuration.of(Map.of("iteration", "500")))
              .offer(
                  AlgorithmConfig.builder()
                      .clusterInfo(ClusterInfo.empty())
                      .clusterBean(metricSource.get())
                      .timeout(Duration.ofSeconds(3))
                      .clusterCost(theCostFunction)
                      .build());
          Assertions.assertTrue(called.get(), "The cost function has been invoked");
        };

    test.accept(0L);
    test.accept(1L);
    test.accept(2L);
    test.accept(3L);
    test.accept(4L);
    test.accept(5L);
    test.accept(6L);
    test.accept(7L);
    test.accept(8L);
    test.accept(9L);
  }
}
