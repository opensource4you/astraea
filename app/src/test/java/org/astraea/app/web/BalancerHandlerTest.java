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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.app.web.BalancerHandler.BalancerPostRequest;
import org.astraea.app.web.BalancerHandler.CostWeight;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoBuilder;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.NoSufficientMetricsException;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.metrics.collector.Fetcher;
import org.astraea.common.metrics.platform.HostMetrics;
import org.astraea.common.metrics.platform.JvmMemory;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

public class BalancerHandlerTest extends RequireBrokerCluster {

  static final String TOPICS_KEY = "topics";
  static final String TIMEOUT_KEY = "timeout";
  static final String MAX_MIGRATE_SIZE_KEY = "maxMigratedSize";
  static final String MAX_MIGRATE_LEADER_KEY = "maxMigratedLeader";
  static final String COST_WEIGHT_KEY = "costWeights";
  static final String BALANCER_IMPLEMENTATION_KEY = "balancer";
  static final String BALANCER_CONFIGURATION_KEY = "balancerConfig";
  static final int TIMEOUT_DEFAULT = 3;

  private static final List<BalancerHandler.CostWeight> defaultIncreasing =
      List.of(costWeight(IncreasingCost.class.getName(), 1));
  private static final List<BalancerHandler.CostWeight> defaultDecreasing =
      List.of(costWeight(DecreasingCost.class.getName(), 1));
  private static final Channel defaultPostPlan =
      httpRequest(Map.of(COST_WEIGHT_KEY, defaultDecreasing));

  @Test
  @Timeout(value = 60)
  void testReport() {
    var topics = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      // make sure all replicas have
      admin
          .clusterInfo(Set.copyOf(topics))
          .toCompletableFuture()
          .join()
          .replicaStream()
          .forEach(r -> Assertions.assertNotEquals(0, r.size()));
      var handler = new BalancerHandler(admin);
      var progress =
          submitPlanGeneration(
              handler,
              Map.of(
                  BALANCER_IMPLEMENTATION_KEY,
                  GreedyBalancer.class.getName(),
                  BALANCER_CONFIGURATION_KEY,
                  Map.of("a", "b"),
                  TIMEOUT_KEY,
                  "1234ms",
                  COST_WEIGHT_KEY,
                  defaultDecreasing));
      var report = progress.plan;
      Assertions.assertNotNull(progress.id);
      Assertions.assertEquals(1234, progress.config.timeoutMs);
      Assertions.assertEquals(GreedyBalancer.class.getName(), progress.config.balancer);
      Assertions.assertNotEquals(0, report.changes.size());
      Assertions.assertTrue(report.cost >= report.newCost.get());
      // "before" should record size
      report.changes.forEach(
          c ->
              c.before.forEach(
                  p -> {
                    // if the topic is generated by this test, it should have data
                    if (topics.contains(c.topic)) Assertions.assertNotEquals(0, p.size);
                    // otherwise, we just check non-null
                    else Assertions.assertNotNull(p.size);
                  }));
      // "after" should NOT record size
      report.changes.stream()
          .flatMap(c -> c.after.stream())
          .forEach(p -> Assertions.assertEquals(Optional.empty(), p.size));
      Assertions.assertTrue(report.cost >= report.newCost.get());
      var sizeMigration =
          report.migrationCosts.stream()
              .filter(x -> x.name.equals(BalancerHandler.MOVED_SIZE))
              .findFirst()
              .get();
      Assertions.assertNotEquals(0, sizeMigration.brokerCosts.size());
      sizeMigration.brokerCosts.values().forEach(v -> Assertions.assertNotEquals(0D, v));
    }
  }

  @Test
  @Timeout(value = 60)
  void testTopics() {
    var topicNames = createAndProduceTopic(5);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin);
      // For all 5 topics, we only allow the first two topics can be altered.
      // We apply this limitation to test if the BalancerHandler.TOPICS_KEY works correctly.
      var allowedTopics = topicNames.subList(0, 2);
      var report =
          submitPlanGeneration(
                  handler,
                  Map.of(
                      BALANCER_CONFIGURATION_KEY,
                      Map.of("iteration", "30"),
                      TOPICS_KEY,
                      String.join(",", allowedTopics),
                      COST_WEIGHT_KEY,
                      defaultDecreasing))
              .plan;
      Assertions.assertTrue(
          report.changes.stream().map(x -> x.topic).allMatch(allowedTopics::contains),
          "Only allowed topics been altered");
      Assertions.assertTrue(
          report.cost >= report.newCost.get(),
          "The proposed plan should has better score then the current one");
      var sizeMigration =
          report.migrationCosts.stream()
              .filter(x -> x.name.equals(BalancerHandler.MOVED_SIZE))
              .findFirst()
              .get();
      Assertions.assertNotEquals(0, sizeMigration.brokerCosts.size());
      Assertions.assertNotEquals(
          0,
          sizeMigration.brokerCosts.values().stream().filter(v -> v > 0).count(),
          "report.cost: " + report.cost + " report.newCost.get(): " + report.newCost.get());
    }
  }

  private static List<String> createAndProduceTopic(int topicCount) {
    return createAndProduceTopic(topicCount, 3, (short) 1, true);
  }

  private static List<String> createAndProduceTopic(
      int topicCount, int partitions, short replicas, boolean skewed) {
    try (var admin = Admin.of(bootstrapServers())) {
      var topics =
          IntStream.range(0, topicCount)
              .mapToObj(ignored -> Utils.randomString(10))
              .collect(Collectors.toUnmodifiableList());
      topics.forEach(
          topic -> {
            admin
                .creator()
                .topic(topic)
                .numberOfPartitions(partitions)
                .numberOfReplicas(replicas)
                .run()
                .toCompletableFuture()
                .join();
            if (skewed) {
              Utils.sleep(Duration.ofSeconds(1));
              var placement =
                  brokerIds().stream().limit(replicas).collect(Collectors.toUnmodifiableList());
              admin
                  .moveToBrokers(
                      admin.topicPartitions(Set.of(topic)).toCompletableFuture().join().stream()
                          .collect(Collectors.toMap(tp -> tp, ignored -> placement)))
                  .toCompletableFuture()
                  .join();
            }
          });
      Utils.sleep(Duration.ofSeconds(3));
      try (var producer = Producer.of(bootstrapServers())) {
        IntStream.range(0, 30)
            .forEach(
                index ->
                    topics.forEach(
                        topic ->
                            producer.send(
                                Record.builder()
                                    .topic(topic)
                                    .key(String.valueOf(index).getBytes(StandardCharsets.UTF_8))
                                    .build())));
      }
      return topics;
    }
  }

  @Test
  @Timeout(value = 60)
  void testBestPlan() {
    try (var admin = Admin.of(bootstrapServers())) {
      var currentClusterInfo =
          ClusterInfo.of(
              List.of(NodeInfo.of(10, "host", 22), NodeInfo.of(11, "host", 22)),
              List.of(
                  Replica.builder()
                      .topic("topic")
                      .partition(0)
                      .nodeInfo(NodeInfo.of(10, "host", 22))
                      .lag(0)
                      .size(100)
                      .isLeader(true)
                      .inSync(true)
                      .isFuture(false)
                      .isOffline(false)
                      .isPreferredLeader(true)
                      .path("/tmp/aa")
                      .build()));

      HasClusterCost clusterCostFunction =
          (clusterInfo, clusterBean) -> () -> clusterInfo == currentClusterInfo ? 100D : 10D;
      HasMoveCost moveCostFunction = HasMoveCost.EMPTY;

      var balancerHandler = new BalancerHandler(admin);
      var Best =
          Balancer.create(
                  SingleStepBalancer.class,
                  AlgorithmConfig.builder()
                      .clusterCost(clusterCostFunction)
                      .clusterConstraint((before, after) -> after.value() <= before.value())
                      .moveCost(moveCostFunction)
                      .movementConstraint(moveCosts -> true)
                      .build())
              .offer(
                  admin
                      .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                      .toCompletableFuture()
                      .join(),
                  Duration.ofSeconds(3));

      Assertions.assertNotEquals(Optional.empty(), Best);

      // test loop limit
      Assertions.assertThrows(
          Exception.class,
          () ->
              Balancer.create(
                      SingleStepBalancer.class,
                      AlgorithmConfig.builder()
                          .clusterCost(clusterCostFunction)
                          .clusterConstraint((before, after) -> true)
                          .moveCost(moveCostFunction)
                          .movementConstraint(moveCosts -> true)
                          .config(Configuration.of(Map.of("iteration", "0")))
                          .build())
                  .offer(
                      admin
                          .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                          .toCompletableFuture()
                          .join(),
                      Duration.ofSeconds(3)));

      // test cluster cost predicate
      Assertions.assertEquals(
          Optional.empty(),
          Balancer.create(
                  SingleStepBalancer.class,
                  AlgorithmConfig.builder()
                      .clusterCost(clusterCostFunction)
                      .clusterConstraint((before, after) -> false)
                      .moveCost(moveCostFunction)
                      .movementConstraint(moveCosts -> true)
                      .build())
              .offer(
                  admin
                      .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                      .toCompletableFuture()
                      .join(),
                  Duration.ofSeconds(3))
              .solution());

      // test move cost predicate
      Assertions.assertEquals(
          Optional.empty(),
          Balancer.create(
                  SingleStepBalancer.class,
                  AlgorithmConfig.builder()
                      .clusterCost(clusterCostFunction)
                      .clusterConstraint((before, after) -> true)
                      .moveCost(moveCostFunction)
                      .movementConstraint(moveCosts -> false)
                      .build())
              .offer(
                  admin
                      .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                      .toCompletableFuture()
                      .join(),
                  Duration.ofSeconds(3))
              .solution());
    }
  }

  @CsvSource(value = {"2,100Byte", "2,500Byte", "2,1GB", "5,100Byte", "5,500Byte", "5,1GB"})
  @ParameterizedTest
  void testMoveCost(String leaderLimit, String sizeLimit) {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin);
      var report =
          submitPlanGeneration(
                  handler,
                  Map.of(
                      MAX_MIGRATE_LEADER_KEY,
                      leaderLimit,
                      MAX_MIGRATE_SIZE_KEY,
                      sizeLimit,
                      COST_WEIGHT_KEY,
                      defaultDecreasing))
              .plan;
      report.migrationCosts.forEach(
          migrationCost -> {
            switch (migrationCost.name) {
              case BalancerHandler.MOVED_SIZE:
                Assertions.assertTrue(
                    migrationCost.brokerCosts.values().stream().mapToLong(Double::intValue).sum()
                        <= DataSize.of(sizeLimit).bytes());
                break;
              case BalancerHandler.CHANGED_LEADERS:
                Assertions.assertTrue(
                    migrationCost.brokerCosts.values().stream().mapToLong(Double::byteValue).sum()
                        <= Integer.parseInt(leaderLimit));
                break;
            }
          });
    }
  }

  @Test
  @Timeout(value = 60)
  void testNoReport() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(1));
      var handler = new BalancerHandler(admin);
      var post =
          Assertions.assertInstanceOf(
              BalancerHandler.PostPlanResponse.class,
              handler
                  .post(
                      httpRequest(
                          Map.of(
                              COST_WEIGHT_KEY,
                              defaultIncreasing,
                              TIMEOUT_KEY,
                              "996ms",
                              BALANCER_IMPLEMENTATION_KEY,
                              GreedyBalancer.class.getName())))
                  .toCompletableFuture()
                  .join());
      Utils.sleep(Duration.ofSeconds(5));
      var progress =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join());
      Assertions.assertNotNull(post.id);
      Assertions.assertEquals(post.id, progress.id);
      Assertions.assertEquals(996, progress.config.timeoutMs);
      Assertions.assertEquals(GreedyBalancer.class.getName(), progress.config.balancer);
      Assertions.assertEquals(BalancerHandler.PlanPhase.Searched, progress.phase, "search done");
      Assertions.assertNotNull(progress.exception, "hint about no plan found");
      Assertions.assertNotNull(progress.config.function);
      Assertions.assertNull(progress.plan, "no proposal");
      Assertions.assertInstanceOf(
          IllegalStateException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      handler
                          .put(httpRequest(Map.of("id", progress.id)))
                          .toCompletableFuture()
                          .join())
              .getCause(),
          "Cannot execute a plan with no proposal available");
    }
  }

  @Test
  @Timeout(value = 60)
  void testPut() {
    // arrange
    createAndProduceTopic(3, 10, (short) 2, false);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor = new NoOpExecutor();
      var handler = new BalancerHandler(admin, theExecutor);
      var progress =
          submitPlanGeneration(
              handler,
              Map.of(
                  COST_WEIGHT_KEY,
                  defaultDecreasing,
                  BALANCER_CONFIGURATION_KEY,
                  Map.of("iteration", "100")));
      var thePlanId = progress.id;

      // act
      var response =
          Assertions.assertInstanceOf(
              BalancerHandler.PutPlanResponse.class,
              handler.put(httpRequest(Map.of("id", thePlanId))).toCompletableFuture().join());
      Utils.sleep(Duration.ofSeconds(1));

      // assert
      Assertions.assertEquals(Response.ACCEPT.code(), response.code());
      Assertions.assertEquals(thePlanId, response.id);
      Assertions.assertEquals(1, theExecutor.count());
    }
  }

  @Test
  @Timeout(value = 60)
  void testBadPut() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin, new NoOpExecutor());

      // no id offered
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> handler.put(Channel.EMPTY).toCompletableFuture().join(),
          "The 'id' field is required");

      // no such plan id
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> handler.put(httpRequest(Map.of("id", "no such plan"))).toCompletableFuture().join(),
          "The requested plan doesn't exists");
    }
  }

  @Test
  @Timeout(value = 360)
  void testSubmitRebalancePlanThreadSafe() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(30).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));
      admin
          .moveToBrokers(
              admin.topicPartitions(Set.of(topic)).toCompletableFuture().join().stream()
                  .collect(Collectors.toMap(Function.identity(), ignored -> List.of(1))))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(3));
      var theExecutor = new NoOpExecutor();
      var handler = new BalancerHandler(admin, theExecutor);
      var progress = submitPlanGeneration(handler, Map.of());

      // use many threads to increase the chance to trigger a data race
      final int threadCount = Runtime.getRuntime().availableProcessors() * 3;
      final var executor = Executors.newFixedThreadPool(threadCount);
      final var barrier = new CyclicBarrier(threadCount);

      // launch threads
      IntStream.range(0, threadCount)
          .forEach(
              ignore ->
                  executor.submit(
                      () -> {
                        // the plan
                        final var request = httpRequest(Map.of("id", progress.id));
                        // use cyclic barrier to ensure all threads are ready to work
                        Utils.packException(() -> barrier.await());
                        // send the put request
                        handler.put(request).toCompletableFuture().join();
                      }));

      // await work done
      executor.shutdown();
      Assertions.assertTrue(
          Utils.packException(() -> executor.awaitTermination(threadCount * 3L, TimeUnit.SECONDS)));

      // the rebalance task is triggered in async manner, it may take some time to getting schedule
      Utils.sleep(Duration.ofMillis(500));
      // test if the plan has been executed just once
      Assertions.assertEquals(1, theExecutor.count());
    }
  }

  @Test
  @Timeout(value = 60)
  void testRebalanceOnePlanAtATime() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor =
          new NoOpExecutor() {
            @Override
            public CompletionStage<Void> run(
                Admin admin, ClusterInfo<Replica> targetAllocation, Duration timeout) {
              return super.run(admin, targetAllocation, Duration.ofSeconds(5))
                  // Use another thread to block this completion to avoid deadlock in
                  // BalancerHandler#put
                  .thenApplyAsync(
                      i -> {
                        Utils.sleep(Duration.ofSeconds(10));
                        return i;
                      });
            }
          };
      var handler = new BalancerHandler(admin, theExecutor);
      var plan0 = submitPlanGeneration(handler, Map.of());
      var plan1 = submitPlanGeneration(handler, Map.of());

      Assertions.assertDoesNotThrow(
          () -> handler.put(httpRequest(Map.of("id", plan0.id))).toCompletableFuture().join());
      Assertions.assertInstanceOf(
          IllegalStateException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      handler.put(httpRequest(Map.of("id", plan1.id))).toCompletableFuture().join())
              .getCause());
    }
  }

  @Test
  @Timeout(value = 60)
  void testRebalanceDetectOngoing() {
    try (var admin = Admin.of(bootstrapServers())) {
      var theTopic = Utils.randomString();
      admin.creator().topic(theTopic).numberOfPartitions(1).run().toCompletableFuture().join();
      try (var producer = Producer.of(bootstrapServers())) {
        var dummy = new byte[1024];
        IntStream.range(0, 100000)
            .mapToObj(i -> producer.send(Record.builder().topic(theTopic).value(dummy).build()))
            .collect(Collectors.toUnmodifiableSet())
            .forEach(i -> i.toCompletableFuture().join());
      }

      var handler = new BalancerHandler(admin, new NoOpExecutor());
      var theReport =
          submitPlanGeneration(
              handler,
              Map.of(
                  TOPICS_KEY, theTopic,
                  COST_WEIGHT_KEY, defaultDecreasing));

      // create an ongoing reassignment
      Assertions.assertEquals(
          1,
          admin.clusterInfo(Set.of(theTopic)).toCompletableFuture().join().replicaStream().count());
      admin
          .moveToBrokers(Map.of(TopicPartition.of(theTopic, 0), List.of(0, 1, 2)))
          .toCompletableFuture()
          .join();

      // debounce wait
      Assertions.assertTrue(
          admin
              .waitCluster(
                  Set.of(theTopic),
                  clusterInfo ->
                      clusterInfo
                          .replicaStream()
                          .noneMatch(r -> r.isFuture() || r.isRemoving() || r.isAdding()),
                  Duration.ofSeconds(10),
                  2)
              .toCompletableFuture()
              .join());

      Assertions.assertInstanceOf(
          IllegalStateException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      handler
                          .put(httpRequest(Map.of("id", theReport.id)))
                          .toCompletableFuture()
                          .join())
              .getCause());
    }
  }

  @Test
  @Timeout(value = 60)
  void testGenerationDetectOngoing() {
    var base =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(Map.of(1, Set.of("/f0", "/f1")))
            .addFolders(Map.of(2, Set.of("/f0", "/f1")))
            .addFolders(Map.of(3, Set.of("/f0", "/f1")))
            .addTopic("A", 10, (short) 1)
            .addTopic("B", 10, (short) 1)
            .addTopic("C", 10, (short) 1)
            .build();
    var iter0 = Stream.iterate(true, (i) -> false).iterator();
    var iter1 = Stream.iterate(true, (i) -> false).iterator();
    var iter2 = Stream.iterate(true, (i) -> false).iterator();
    var clusterHasFuture =
        ClusterInfoBuilder.builder(base)
            .mapLog(r -> Replica.builder(r).isFuture(iter0.next()).build())
            .build();
    var clusterHasAdding =
        ClusterInfoBuilder.builder(base)
            .mapLog(r -> Replica.builder(r).isAdding(iter1.next()).build())
            .build();
    var clusterHasRemoving =
        ClusterInfoBuilder.builder(base)
            .mapLog(r -> Replica.builder(r).isRemoving(iter2.next()).build())
            .build();
    try (Admin admin = Mockito.mock(Admin.class)) {
      Mockito.when(admin.topicNames(Mockito.anyBoolean()))
          .thenAnswer((invoke) -> CompletableFuture.completedFuture(Set.of("A", "B", "C")));

      Mockito.when(admin.clusterInfo(Mockito.any()))
          .thenAnswer((invoke) -> CompletableFuture.completedFuture(clusterHasFuture));
      Assertions.assertThrows(
          IllegalStateException.class, () -> new BalancerHandler(admin).post(defaultPostPlan));

      Mockito.when(admin.clusterInfo(Mockito.any()))
          .thenAnswer((invoke) -> CompletableFuture.completedFuture(clusterHasAdding));
      Assertions.assertThrows(
          IllegalStateException.class, () -> new BalancerHandler(admin).post(defaultPostPlan));

      Mockito.when(admin.clusterInfo(Mockito.any()))
          .thenAnswer((invoke) -> CompletableFuture.completedFuture(clusterHasRemoving));
      Assertions.assertThrows(
          IllegalStateException.class, () -> new BalancerHandler(admin).post(defaultPostPlan));

      Mockito.when(admin.clusterInfo(Mockito.any()))
          .thenAnswer((invoke) -> CompletableFuture.completedFuture(base));
      Assertions.assertDoesNotThrow(() -> new BalancerHandler(admin).post(defaultPostPlan));
    }
  }

  @Test
  @Timeout(value = 60)
  void testPutSanityCheck() {
    var topic = createAndProduceTopic(1).get(0);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor = new NoOpExecutor();
      var handler = new BalancerHandler(admin, theExecutor);
      var theProgress =
          submitPlanGeneration(
              handler,
              Map.of(
                  COST_WEIGHT_KEY, defaultDecreasing,
                  TOPICS_KEY, topic));

      // pick a partition and alter its placement
      var theChange = theProgress.plan.changes.stream().findAny().orElseThrow();
      admin
          .moveToBrokers(
              Map.of(TopicPartition.of(theChange.topic, theChange.partition), List.of(0, 1, 2)))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(10));

      // assert
      Assertions.assertInstanceOf(
          IllegalStateException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      handler
                          .put(httpRequest(Map.of("id", theProgress.id)))
                          .toCompletableFuture()
                          .join(),
                  "The cluster state has changed, prevent the plan from execution")
              .getCause());
    }
  }

  @Test
  @Timeout(value = 60)
  void testLookupRebalanceProgress() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor =
          new NoOpExecutor() {
            final CountDownLatch latch = new CountDownLatch(1);

            @Override
            public CompletionStage<Void> run(
                Admin admin, ClusterInfo<Replica> targetAllocation, Duration timeout) {
              return super.run(admin, targetAllocation, Duration.ofSeconds(5))
                  // Use another thread to block this completion to avoid deadlock in
                  // BalancerHandler#put
                  .thenApplyAsync(
                      i -> {
                        Utils.packException(() -> latch.await());
                        return i;
                      });
            }
          };
      var handler = new BalancerHandler(admin, theExecutor);
      var progress = submitPlanGeneration(handler, Map.of());
      Assertions.assertEquals(BalancerHandler.PlanPhase.Searched, progress.phase);

      // not scheduled yet
      Utils.sleep(Duration.ofSeconds(1));
      var progress0 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(progress.id)).toCompletableFuture().join());
      Assertions.assertEquals(progress.id, progress0.id);
      Assertions.assertEquals(BalancerHandler.PlanPhase.Searched, progress0.phase);
      Assertions.assertNull(progress0.exception);

      // schedule
      var response =
          Assertions.assertInstanceOf(
              BalancerHandler.PutPlanResponse.class,
              handler.put(httpRequest(Map.of("id", progress.id))).toCompletableFuture().join());
      Assertions.assertNotNull(response.id, "The plan should be executed");

      // not done yet
      Utils.sleep(Duration.ofSeconds(1));
      var progress1 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(response.id)).toCompletableFuture().join());
      Assertions.assertEquals(progress.id, progress1.id);
      Assertions.assertEquals(BalancerHandler.PlanPhase.Executing, progress1.phase);
      Assertions.assertNull(progress1.exception);

      // it is done
      theExecutor.latch.countDown();
      Utils.sleep(Duration.ofSeconds(1));
      var progress2 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(response.id)).toCompletableFuture().join());
      Assertions.assertEquals(progress.id, progress2.id);
      Assertions.assertEquals(BalancerHandler.PlanPhase.Executed, progress2.phase);
      Assertions.assertNull(progress2.exception);
    }
  }

  @Test
  @Timeout(value = 60)
  void testLookupBadExecutionProgress() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor =
          new NoOpExecutor() {
            @Override
            public CompletionStage<Void> run(
                Admin admin, ClusterInfo<Replica> targetAllocation, Duration timeout) {
              return super.run(admin, targetAllocation, Duration.ofSeconds(5))
                  .thenCompose(
                      ignored -> CompletableFuture.failedFuture(new RuntimeException("Boom")));
            }
          };
      var handler = new BalancerHandler(admin, theExecutor);

      var post =
          Assertions.assertInstanceOf(
              BalancerHandler.PostPlanResponse.class,
              handler
                  .post(httpRequest(Map.of(COST_WEIGHT_KEY, defaultDecreasing)))
                  .toCompletableFuture()
                  .join());
      Utils.waitFor(
          () ->
              ((BalancerHandler.PlanExecutionProgress)
                      handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join())
                  .phase.calculated());
      var generated =
          ((BalancerHandler.PlanExecutionProgress)
                      handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join())
                  .phase
              == BalancerHandler.PlanPhase.Searched;
      Assertions.assertTrue(generated, "The plan should be generated");

      var progress0 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join());
      Assertions.assertEquals(
          BalancerHandler.PlanPhase.Searched, progress0.phase, "The plan is ready");

      // schedule
      var response =
          Assertions.assertInstanceOf(
              BalancerHandler.PutPlanResponse.class,
              handler.put(httpRequest(Map.of("id", post.id))).toCompletableFuture().join());
      Assertions.assertNotNull(response.id, "The plan should be executed");

      // exception
      Utils.sleep(Duration.ofSeconds(1));
      var progress =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(response.id)).toCompletableFuture().join());
      Assertions.assertEquals(post.id, progress.id);
      Assertions.assertNotNull(progress.exception);
      Assertions.assertEquals(BalancerHandler.PlanPhase.Executed, progress.phase);
      Assertions.assertInstanceOf(String.class, progress.exception);
    }
  }

  @Test
  @Timeout(value = 60)
  void testBadLookupRequest() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin, new NoOpExecutor());

      Assertions.assertEquals(
          404, handler.get(Channel.ofTarget("no such plan")).toCompletableFuture().join().code());

      // plan doesn't exists
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> handler.put(httpRequest(Map.of("id", "no such plan"))).toCompletableFuture().join(),
          "This plan doesn't exists");
    }
  }

  @Test
  @Timeout(value = 60)
  void testPutIdempotent() {
    var topics = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin, new StraightPlanExecutor());
      var progress =
          submitPlanGeneration(
              handler,
              Map.of(COST_WEIGHT_KEY, defaultDecreasing, TOPICS_KEY, String.join(",", topics)));

      Assertions.assertDoesNotThrow(
          () -> handler.put(httpRequest(Map.of("id", progress.id))).toCompletableFuture().join(),
          "Schedule the rebalance task");

      // Wait until the migration occurred
      try {
        Utils.waitFor(
            () ->
                admin
                    .clusterInfo(Set.copyOf(topics))
                    .toCompletableFuture()
                    .join()
                    .replicaStream()
                    .anyMatch(replica -> replica.isFuture() || !replica.inSync()));
      } catch (Exception ignore) {
      }

      Assertions.assertDoesNotThrow(
          () -> handler.put(httpRequest(Map.of("id", progress.id))).toCompletableFuture().join(),
          "Idempotent behavior");
    }
  }

  @Test
  @Timeout(value = 60)
  void testCustomBalancer() {
    var topics = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin, new StraightPlanExecutor());
      var balancer = SpyBalancer.class.getName();
      var balancerConfig =
          Map.of(
              "key0", "value0",
              "key1", "value1",
              "key2", "value2");

      var newInvoked = new AtomicBoolean(false);
      var offerInvoked = new AtomicBoolean(false);
      SpyBalancer.offerCallbacks.add(() -> offerInvoked.set(true));
      SpyBalancer.newCallbacks.add(
          (algorithmConfig) -> {
            Assertions.assertEquals("value0", algorithmConfig.config().requireString("key0"));
            Assertions.assertEquals("value1", algorithmConfig.config().requireString("key1"));
            Assertions.assertEquals("value2", algorithmConfig.config().requireString("key2"));
            newInvoked.set(true);
          });

      var progress =
          submitPlanGeneration(
              handler,
              Map.of(
                  BALANCER_IMPLEMENTATION_KEY,
                  balancer,
                  BALANCER_CONFIGURATION_KEY,
                  balancerConfig,
                  COST_WEIGHT_KEY,
                  defaultDecreasing,
                  TOPICS_KEY,
                  String.join(",", topics)));

      Assertions.assertEquals(BalancerHandler.PlanPhase.Searched, progress.phase, "Plan is here");
      Assertions.assertTrue(newInvoked.get(), "The customized balancer is created");
      Assertions.assertTrue(offerInvoked.get(), "The customized balancer is used");
    }
  }

  @Test
  void testParsePostRequest() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var clusterInfo =
          admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();
      {
        // no cost weight
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> BalancerHandler.parsePostRequestWrapper(new BalancerPostRequest(), clusterInfo),
            "CostWeights must be specified");
      }
      {
        // minimal
        var request = new BalancerPostRequest();
        request.costWeights = List.of(costWeight(DecreasingCost.class.getName(), 1));
        var postRequest = BalancerHandler.parsePostRequestWrapper(request, clusterInfo);
        var config = postRequest.algorithmConfig;
        Assertions.assertTrue(config.config().entrySet().isEmpty());
        Assertions.assertInstanceOf(HasClusterCost.class, config.clusterCostFunction());
        Assertions.assertTrue(config.clusterCostFunction().toString().contains("DecreasingCost"));
        Assertions.assertTrue(config.clusterCostFunction().toString().contains("weight 1"));
        Assertions.assertEquals(TIMEOUT_DEFAULT, postRequest.executionTime.toSeconds());
        Assertions.assertTrue(
            clusterInfo.topics().stream().allMatch(t -> config.topicFilter().test(t)));
      }
      {
        // use custom filter/timeout/balancer config/cost function
        var randomTopic0 = Utils.randomString();
        var randomTopic1 = Utils.randomString();
        var request = new BalancerPostRequest();
        request.topics = Optional.of(randomTopic0 + "," + randomTopic1);
        request.timeout = "32";
        request.balancerConfig = Map.of("KEY", "VALUE");
        request.costWeights = List.of(costWeight(DecreasingCost.class.getName(), 1));

        var postRequest = BalancerHandler.parsePostRequestWrapper(request, clusterInfo);
        var config = postRequest.algorithmConfig;
        Assertions.assertEquals(Set.of(Map.entry("KEY", "VALUE")), config.config().entrySet());
        Assertions.assertInstanceOf(HasClusterCost.class, config.clusterCostFunction());
        Assertions.assertEquals(
            1.0, config.clusterCostFunction().clusterCost(clusterInfo, ClusterBean.EMPTY).value());
        Assertions.assertEquals(
            1.0, config.clusterCostFunction().clusterCost(clusterInfo, ClusterBean.EMPTY).value());
        Assertions.assertEquals(
            1.0, config.clusterCostFunction().clusterCost(clusterInfo, ClusterBean.EMPTY).value());
        Assertions.assertEquals(32, postRequest.executionTime.toSeconds());
        Assertions.assertTrue(config.topicFilter().test(randomTopic0));
        Assertions.assertTrue(config.topicFilter().test(randomTopic1));
        Assertions.assertTrue(
            clusterInfo.topics().stream().noneMatch(t -> config.topicFilter().test(t)));
      }
      {
        // malformed content
        var request0 =
            Map.of(
                TOPICS_KEY,
                "",
                TIMEOUT_KEY,
                32,
                BALANCER_CONFIGURATION_KEY,
                Map.of("KEY", "VALUE"),
                COST_WEIGHT_KEY,
                defaultDecreasing);
        var balancerRequest = new BalancerPostRequest();
        balancerRequest.topics = Optional.of("");
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> BalancerHandler.parsePostRequestWrapper(balancerRequest, clusterInfo),
            "Empty topic filter, nothing to rebalance");

        var balancerRequest3 = new BalancerPostRequest();
        balancerRequest3.timeout = "-5566";
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> BalancerHandler.parsePostRequestWrapper(balancerRequest3, clusterInfo),
            "Negative timeout");

        var balancerRequest4 = new BalancerPostRequest();
        var costWeight = new CostWeight();
        costWeight.cost = Optional.of("yes");
        balancerRequest4.costWeights = List.of(costWeight);
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> BalancerHandler.parsePostRequestWrapper(balancerRequest4, clusterInfo),
            "Malformed cost weight");
      }
    }
  }

  @Test
  void testTimeout() {
    createAndProduceTopic(5);
    try (var admin = Admin.of(bootstrapServers())) {
      var costFunction = Collections.singleton(costWeight(TimeoutCost.class.getName(), 1));
      var handler = new BalancerHandler(admin, (ignore) -> Optional.of(jmxServiceURL().getPort()));
      var channel = httpRequest(Map.of(TIMEOUT_KEY, "10", COST_WEIGHT_KEY, costFunction));
      var post =
          (BalancerHandler.PostPlanResponse) handler.post(channel).toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(11));

      var progress =
          (BalancerHandler.PlanExecutionProgress)
              handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join();
      Assertions.assertEquals(BalancerHandler.PlanPhase.Searched, progress.phase);
      Assertions.assertNotNull(
          progress.exception, "The generation timeout and failed with some reason");
    }
  }

  @Test
  void testCostWithFetcher() {
    var topics = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var invoked = new AtomicBoolean();
      var handler = new BalancerHandler(admin, (ignore) -> Optional.of(jmxServiceURL().getPort()));
      FetcherAndCost.callback.set(
          (clusterBean) -> {
            var metrics =
                clusterBean.all().values().stream()
                    .flatMap(Collection::stream)
                    .filter(x -> x instanceof JvmMemory)
                    .collect(Collectors.toUnmodifiableSet());
            if (metrics.size() < 3)
              throw new NoSufficientMetricsException(
                  new FetcherAndCost(null), Duration.ofSeconds(3));
            metrics.forEach(i -> Assertions.assertInstanceOf(JvmMemory.class, i));
            invoked.set(true);
          });
      var fetcherAndCost = Collections.singleton(costWeight(FetcherAndCost.class.getName(), 1));

      var progress =
          submitPlanGeneration(
              handler,
              Map.of(
                  TIMEOUT_KEY,
                  "8",
                  COST_WEIGHT_KEY,
                  fetcherAndCost,
                  TOPICS_KEY,
                  String.join(",", topics)));

      Assertions.assertEquals(BalancerHandler.PlanPhase.Searched, progress.phase);
      Assertions.assertTrue(invoked.get());
    }
  }

  @Test
  void testFreshJmxAddress() {
    try (var admin = Admin.of(bootstrapServers())) {
      var noJmx = new BalancerHandler(admin, (id) -> Optional.empty());
      var withJmx = new BalancerHandler(admin, (id) -> Optional.of(5566));
      var partialJmx =
          new BalancerHandler(admin, (id) -> Optional.ofNullable(id != 0 ? 1000 : null));

      Assertions.assertEquals(0, noJmx.freshJmxAddresses().size());
      Assertions.assertEquals(3, withJmx.freshJmxAddresses().size());
      Assertions.assertThrows(IllegalArgumentException.class, partialJmx::freshJmxAddresses);
    }
  }

  @Test
  void testChangeOrder() {
    // arrange
    var sourcePlacement =
        IntStream.range(0, 10)
            .mapToObj(partition -> Map.entry(ThreadLocalRandom.current().nextInt(), partition))
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .collect(Collectors.toUnmodifiableList());
    var destPlacement =
        IntStream.range(0, 10)
            .mapToObj(partition -> Map.entry(ThreadLocalRandom.current().nextInt(), partition))
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .collect(Collectors.toUnmodifiableList());
    var base =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
            .addFolders(Map.of(0, Set.of("/folder0", "/folder1", "/folder2")))
            .addFolders(Map.of(1, Set.of("/folder0", "/folder1", "/folder2")))
            .addFolders(Map.of(2, Set.of("/folder0", "/folder1", "/folder2")))
            .addFolders(Map.of(3, Set.of("/folder0", "/folder1", "/folder2")))
            .addFolders(Map.of(4, Set.of("/folder0", "/folder1", "/folder2")))
            .addFolders(Map.of(5, Set.of("/folder0", "/folder1", "/folder2")))
            .addFolders(Map.of(6, Set.of("/folder0", "/folder1", "/folder2")))
            .addFolders(Map.of(7, Set.of("/folder0", "/folder1", "/folder2")))
            .addFolders(Map.of(8, Set.of("/folder0", "/folder1", "/folder2")))
            .addFolders(Map.of(9, Set.of("/folder0", "/folder1", "/folder2")))
            .build();
    var srcIter = sourcePlacement.iterator();
    var srcPrefIter = Stream.iterate(true, (ignore) -> false).iterator();
    var srcDirIter = Stream.generate(() -> "/folder0").iterator();
    var sourceCluster =
        ClusterInfoBuilder.builder(base)
            .addTopic(
                "Pipeline",
                1,
                (short) 10,
                r ->
                    Replica.builder(r)
                        .nodeInfo(base.node(srcIter.next()))
                        .isPreferredLeader(srcPrefIter.next())
                        .path(srcDirIter.next())
                        .build())
            .build();
    var dstIter = destPlacement.iterator();
    var dstPrefIter = Stream.iterate(true, (ignore) -> false).iterator();
    var dstDirIter = Stream.generate(() -> "/folder1").iterator();
    var destCluster =
        ClusterInfoBuilder.builder(base)
            .addTopic(
                "Pipeline",
                1,
                (short) 10,
                r ->
                    Replica.builder(r)
                        .nodeInfo(base.node(dstIter.next()))
                        .isPreferredLeader(dstPrefIter.next())
                        .path(dstDirIter.next())
                        .build())
            .build();

    // act
    var change =
        BalancerHandler.Change.from(
            sourceCluster.replicas("Pipeline"), destCluster.replicas("Pipeline"));

    // assert
    Assertions.assertEquals("Pipeline", change.topic);
    Assertions.assertEquals(0, change.partition);
    Assertions.assertEquals(
        sourcePlacement.get(0), change.before.get(0).brokerId, "First replica is preferred leader");
    Assertions.assertEquals(
        destPlacement.get(0), change.after.get(0).brokerId, "First replica is preferred leader");
    Assertions.assertEquals(
        Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
        change.before.stream().map(x -> x.brokerId).collect(Collectors.toUnmodifiableSet()),
        "No node ignored");
    Assertions.assertEquals(
        Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
        change.after.stream().map(x -> x.brokerId).collect(Collectors.toUnmodifiableSet()),
        "No node ignored");
    Assertions.assertTrue(
        change.before.stream().map(x -> x.directory).allMatch(x -> x.equals("/folder0")),
        "Correct folder");
    Assertions.assertTrue(
        change.after.stream().map(x -> x.directory).allMatch(x -> x.equals("/folder1")),
        "Correct folder");
    Assertions.assertTrue(change.before.stream().allMatch(x -> x.size.isPresent()), "Has size");
    Assertions.assertTrue(change.after.stream().noneMatch(x -> x.size.isPresent()), "No size");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            BalancerHandler.Change.from(
                ClusterInfoBuilder.builder(base)
                    .addTopic("Pipeline", 1, (short) 3)
                    .build()
                    .replicas(),
                ClusterInfoBuilder.builder(base)
                    .addTopic("Pipeline", 5, (short) 3)
                    .build()
                    .replicas()),
        "Should be a replica list");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            BalancerHandler.Change.from(
                ClusterInfoBuilder.builder(base)
                    .addTopic("Pipeline", 5, (short) 3)
                    .build()
                    .replicas(),
                ClusterInfoBuilder.builder(base)
                    .addTopic("Pipeline", 1, (short) 3)
                    .build()
                    .replicas()),
        "Should be a replica list");
    Assertions.assertThrows(
        NoSuchElementException.class,
        () -> BalancerHandler.Change.from(sourceCluster.replicas(), Set.of()));
    Assertions.assertThrows(
        NoSuchElementException.class,
        () -> BalancerHandler.Change.from(Set.of(), sourceCluster.replicas()));
    Assertions.assertThrows(
        NoSuchElementException.class, () -> BalancerHandler.Change.from(Set.of(), Set.of()));
  }

  /** Submit the plan and wait until it generated. */
  private BalancerHandler.PlanExecutionProgress submitPlanGeneration(
      BalancerHandler handler, Map<String, Object> requestBody) {
    if (!requestBody.containsKey(COST_WEIGHT_KEY)) {
      requestBody = new HashMap<>(requestBody);
      requestBody.put(COST_WEIGHT_KEY, defaultDecreasing);
    }
    var post =
        (BalancerHandler.PostPlanResponse)
            handler.post(httpRequest(requestBody)).toCompletableFuture().join();
    Utils.waitFor(
        () -> {
          var progress =
              (BalancerHandler.PlanExecutionProgress)
                  handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join();
          Assertions.assertNull(progress.exception, progress.exception);
          return progress.phase.calculated();
        });
    return (BalancerHandler.PlanExecutionProgress)
        handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join();
  }

  private static class NoOpExecutor implements RebalancePlanExecutor {

    private final LongAdder executionCounter = new LongAdder();

    @Override
    public CompletionStage<Void> run(
        Admin admin, ClusterInfo<Replica> targetAllocation, Duration timeout) {
      executionCounter.increment();
      return CompletableFuture.completedFuture(null);
    }

    int count() {
      return executionCounter.intValue();
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

  public static class IncreasingCost implements HasClusterCost {

    private ClusterInfo<Replica> original;

    public IncreasingCost(Configuration configuration) {}

    private double value0 = 1.0;

    @Override
    public synchronized ClusterCost clusterCost(
        ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
      if (original == null) original = clusterInfo;
      if (ClusterInfo.findNonFulfilledAllocation(original, clusterInfo).isEmpty()) return () -> 1;
      double theCost = value0;
      value0 = value0 * 1.002;
      return () -> theCost;
    }
  }

  public static class FetcherAndCost extends DecreasingCost {

    static AtomicReference<Consumer<ClusterBean>> callback = new AtomicReference<>();

    public FetcherAndCost(Configuration configuration) {
      super(configuration);
    }

    @Override
    public Optional<Fetcher> fetcher() {
      return Optional.of((c) -> List.of(HostMetrics.jvmMemory(c)));
    }

    @Override
    public synchronized ClusterCost clusterCost(
        ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
      callback.get().accept(clusterBean);
      return super.clusterCost(clusterInfo, clusterBean);
    }
  }

  public static class TimeoutCost implements HasClusterCost {
    @Override
    public ClusterCost clusterCost(ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
      throw new NoSufficientMetricsException(this, Duration.ofSeconds(10));
    }
  }

  public static class SpyBalancer extends SingleStepBalancer {

    public static List<Consumer<AlgorithmConfig>> newCallbacks =
        Collections.synchronizedList(new ArrayList<>());
    public static List<Runnable> offerCallbacks = Collections.synchronizedList(new ArrayList<>());

    public SpyBalancer(AlgorithmConfig algorithmConfig) {
      super(algorithmConfig);
      newCallbacks.forEach(c -> c.accept(algorithmConfig));
      newCallbacks.clear();
    }

    @Override
    public Plan offer(ClusterInfo<Replica> currentClusterInfo, Duration timeout) {
      offerCallbacks.forEach(Runnable::run);
      offerCallbacks.clear();
      return super.offer(currentClusterInfo, timeout);
    }
  }

  private static BalancerHandler.CostWeight costWeight(String cost, double weight) {
    var cw = new BalancerHandler.CostWeight();
    cw.cost = Optional.of(cost);
    cw.weight = Optional.of(weight);
    return cw;
  }

  private static Channel httpRequest(Map<String, ?> payload) {
    return Channel.ofRequest(JsonConverter.defaultConverter().toJson(payload));
  }
}
