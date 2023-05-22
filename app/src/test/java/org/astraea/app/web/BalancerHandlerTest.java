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

import static org.astraea.common.balancer.BalancerConsole.TaskPhase.Executed;
import static org.astraea.common.balancer.BalancerConsole.TaskPhase.Executing;
import static org.astraea.common.balancer.BalancerConsole.TaskPhase.ExecutionFailed;
import static org.astraea.common.balancer.BalancerConsole.TaskPhase.SearchFailed;
import static org.astraea.common.balancer.BalancerConsole.TaskPhase.Searched;
import static org.astraea.common.cost.MigrationCost.REPLICA_LEADERS_TO_ADDED;
import static org.astraea.common.cost.MigrationCost.REPLICA_LEADERS_TO_REMOVE;
import static org.astraea.common.cost.MigrationCost.TO_FETCH_BYTES;
import static org.astraea.common.cost.MigrationCost.TO_SYNC_BYTES;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.app.web.BalancerHandler.BalancerPostRequest;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.BalancerConfigs;
import org.astraea.common.balancer.BalancerProblemFormat;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.CostFunction;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.NoSufficientMetricsException;
import org.astraea.common.cost.RecordSizeCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.collector.MetricSensor;
import org.astraea.common.metrics.collector.MetricStore;
import org.astraea.common.metrics.platform.HostMetrics;
import org.astraea.common.metrics.platform.JvmMemory;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

public class BalancerHandlerTest {

  private static final int numberOfBrokers = 3;
  static final String TIMEOUT_KEY = "timeout";
  static final String CLUSTER_COSTS_KEY = "clusterCosts";
  static final String BALANCER_IMPLEMENTATION_KEY = "balancer";
  static final int TIMEOUT_DEFAULT = 3;
  private Service SERVICE;

  private static final List<BalancerProblemFormat.CostWeight> defaultIncreasing =
      List.of(costWeight(IncreasingCost.class.getName(), 1));
  private static final List<BalancerProblemFormat.CostWeight> defaultDecreasing =
      List.of(costWeight(DecreasingCost.class.getName(), 1));
  private static final Channel defaultPostPlan =
      httpRequest(Map.of(CLUSTER_COSTS_KEY, defaultDecreasing));

  @BeforeEach
  public void initService() {
    SERVICE = Service.builder().numberOfBrokers(numberOfBrokers).build();
  }

  @AfterEach
  public void closeService() {
    SERVICE.close();
  }

  @Test
  @Timeout(value = 60)
  void testReport() {
    var topics = createAndProduceTopic(3, SERVICE);
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      var handler = new BalancerHandler(admin, metricStore(admin, Set.of()));
      // make sure all replicas have
      admin
          .clusterInfo(Set.copyOf(topics))
          .toCompletableFuture()
          .join()
          .replicaStream()
          .forEach(r -> Assertions.assertNotEquals(0, r.size()));
      var request = new BalancerPostRequest();
      request.balancer = GreedyBalancer.class.getName();
      request.balancerConfig = Map.of("a", "b");
      request.moveCosts = Set.of("org.astraea.common.cost.RecordSizeCost");
      request.costConfig = Map.of(RecordSizeCost.class.getName(), "10GB");
      request.timeout = Duration.ofMillis(1234);
      var progress = submitPlanGeneration(handler, request);
      var report = progress.plan;
      Assertions.assertNotNull(progress.id);
      Assertions.assertEquals(Duration.ofMillis(1234), progress.config.timeout);
      Assertions.assertEquals(GreedyBalancer.class.getName(), progress.config.balancer);
      Assertions.assertNotEquals(0, report.changes.size());
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
      var sizeMigration =
          report.migrationCosts.stream()
              .filter(x -> x.name.equals(TO_SYNC_BYTES))
              .findFirst()
              .get();
      Assertions.assertNotEquals(0, sizeMigration.brokerCosts.size());
    }
  }

  private static Set<String> createAndProduceTopic(int topicCount, Service service) {
    return createAndProduceTopic(topicCount, 3, (short) 1, true, service);
  }

  private static Set<String> createAndProduceTopic(
      int topicCount, int partitions, short replicas, boolean skewed, Service service) {
    try (var admin = Admin.of(service.bootstrapServers())) {
      var topics =
          IntStream.range(0, topicCount)
              .mapToObj(ignored -> Utils.randomString(10))
              .collect(Collectors.toUnmodifiableSet());
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
              var placement = service.dataFolders().keySet().stream().limit(replicas).toList();
              admin
                  .moveToBrokers(
                      admin.topicPartitions(Set.of(topic)).toCompletableFuture().join().stream()
                          .collect(Collectors.toMap(tp -> tp, ignored -> placement)))
                  .toCompletableFuture()
                  .join();
            }
          });
      Utils.sleep(Duration.ofSeconds(3));
      try (var producer = Producer.of(service.bootstrapServers())) {
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
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      var currentClusterInfo =
          ClusterInfo.builder()
              .addNode(Set.of(1, 2))
              .addFolders(
                  Map.ofEntries(Map.entry(1, Set.of("/folder")), Map.entry(2, Set.of("/folder"))))
              .addTopic("topic", 1, (short) 1)
              .build();

      HasClusterCost clusterCostFunction =
          (clusterInfo, clusterBean) ->
              () ->
                  ClusterInfo.findNonFulfilledAllocation(currentClusterInfo, clusterInfo).isEmpty()
                      ? 100D
                      : 10D;
      HasMoveCost moveCostFunction = HasMoveCost.EMPTY;
      HasMoveCost failMoveCostFunction = (before, after, clusterBean) -> () -> true;

      var Best =
          Utils.construct(SingleStepBalancer.class, Configuration.EMPTY)
              .offer(
                  AlgorithmConfig.builder()
                      .clusterInfo(currentClusterInfo)
                      .clusterBean(ClusterBean.EMPTY)
                      .timeout(Duration.ofSeconds(3))
                      .clusterCost(clusterCostFunction)
                      .moveCost(moveCostFunction)
                      .build());
      Assertions.assertNotEquals(Optional.empty(), Best);

      // test loop limit
      Assertions.assertThrows(
          Exception.class,
          () ->
              Utils.construct(SingleStepBalancer.class, Configuration.EMPTY)
                  .offer(
                      AlgorithmConfig.builder()
                          .clusterInfo(currentClusterInfo)
                          .clusterBean(ClusterBean.EMPTY)
                          .timeout(Duration.ofSeconds(3))
                          .clusterCost(clusterCostFunction)
                          .config("iteration", "0")
                          .moveCost(moveCostFunction)
                          .build()));

      // test cluster cost predicate
      Assertions.assertEquals(
          Optional.empty(),
          Utils.construct(SingleStepBalancer.class, Configuration.EMPTY)
              .offer(
                  AlgorithmConfig.builder()
                      .clusterInfo(
                          admin
                              .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                              .toCompletableFuture()
                              .join())
                      .clusterBean(ClusterBean.EMPTY)
                      .timeout(Duration.ofSeconds(3))
                      .clusterCost(clusterCostFunction)
                      .moveCost(moveCostFunction)
                      .build()));

      // test move cost predicate
      Assertions.assertEquals(
          Optional.empty(),
          Utils.construct(SingleStepBalancer.class, Configuration.EMPTY)
              .offer(
                  AlgorithmConfig.builder()
                      .clusterInfo(currentClusterInfo)
                      .clusterBean(ClusterBean.EMPTY)
                      .timeout(Duration.ofSeconds(3))
                      .clusterCost(clusterCostFunction)
                      .moveCost(failMoveCostFunction)
                      .build()));
    }
  }

  @CsvSource(value = {"5,500Byte", "10,500Byte", "5,1GB"})
  @ParameterizedTest
  void testMoveCost(String leaderLimit, String sizeLimit) {
    createAndProduceTopic(3, SERVICE);
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      var request = new BalancerHandler.BalancerPostRequest();
      request.moveCosts =
          Set.of(
              "org.astraea.common.cost.ReplicaLeaderCost",
              "org.astraea.common.cost.RecordSizeCost");
      request.costConfig =
          Map.of(
              ReplicaLeaderCost.MAX_MIGRATE_LEADER_KEY,
              leaderLimit,
              RecordSizeCost.MAX_MIGRATE_SIZE_KEY,
              sizeLimit);
      Assertions.assertEquals(2, request.moveCosts.size());
      var report = submitPlanGeneration(handler, request).plan;
      report.migrationCosts.forEach(
          migrationCost -> {
            switch (migrationCost.name) {
              case TO_SYNC_BYTES:
              case TO_FETCH_BYTES:
                Assertions.assertTrue(
                    migrationCost.brokerCosts.values().stream().mapToLong(Long::intValue).sum()
                        <= DataSize.of(sizeLimit).bytes());
                break;
              case REPLICA_LEADERS_TO_ADDED:
              case REPLICA_LEADERS_TO_REMOVE:
                Assertions.assertTrue(
                    migrationCost.brokerCosts.values().stream().mapToLong(Long::intValue).sum()
                        <= Long.parseLong(leaderLimit));
                break;
            }
          });
    }
    SERVICE.close();
  }

  @Test
  @Timeout(value = 60)
  void testNoReport() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(1));
      var post =
          Assertions.assertInstanceOf(
              BalancerHandler.PostPlanResponse.class,
              handler
                  .post(
                      httpRequest(
                          Map.of(
                              CLUSTER_COSTS_KEY,
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
      Assertions.assertEquals(Duration.ofMillis(996), progress.config.timeout);
      Assertions.assertEquals(GreedyBalancer.class.getName(), progress.config.balancer);
      Assertions.assertEquals(SearchFailed, progress.phase, "search done");
      Assertions.assertNotNull(progress.exception, "hint about no plan found");
      Assertions.assertNotNull(progress.config.function);
      Assertions.assertNull(progress.plan, "no proposal");
      handler.put(httpRequest(Map.of("id", progress.id))).toCompletableFuture().join();
      Utils.sleep(Duration.ofMillis(300));
      var progress1 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join());
      Assertions.assertEquals(SearchFailed, progress1.phase, "No plan");
      Assertions.assertNotNull(progress1.exception);
    }
    SERVICE.close();
  }

  @Test
  @Timeout(value = 60)
  void testPut() {
    // arrange
    createAndProduceTopic(3, 10, (short) 2, false, SERVICE);
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      var request = new BalancerHandler.BalancerPostRequest();
      request.balancerConfig = Map.of("iteration", "100");
      var progress = submitPlanGeneration(handler, request);
      var thePlanId = progress.id;

      // act
      var response =
          Assertions.assertInstanceOf(
              BalancerHandler.PutPlanResponse.class,
              handler
                  .put(
                      httpRequest(
                          Map.of("id", thePlanId, "executor", NoOpExecutor.class.getName())))
                  .toCompletableFuture()
                  .join());
      Utils.sleep(Duration.ofSeconds(1));

      // assert
      Assertions.assertEquals(Response.ACCEPT.code(), response.code());
      Assertions.assertEquals(thePlanId, response.id);
    }
    SERVICE.close();
  }

  @Test
  @Timeout(value = 60)
  void testBadPut() {
    createAndProduceTopic(3, SERVICE);
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {

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
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      admin.creator().topic(topic).numberOfPartitions(30).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));
      admin
          .moveToBrokers(
              admin.topicPartitions(Set.of(topic)).toCompletableFuture().join().stream()
                  .collect(Collectors.toMap(Function.identity(), ignored -> List.of(1))))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(3));
      var progress = submitPlanGeneration(handler, new BalancerPostRequest());

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
                        final var request =
                            httpRequest(
                                Map.of(
                                    "id", progress.id, "executor", NoOpExecutor.class.getName()));
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
      Utils.sleep(Duration.ofSeconds(2));
    }
    SERVICE.close();
  }

  @Test
  @Timeout(value = 60)
  void testRebalanceDetectOngoing() {
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      // create topic
      var theTopic = Utils.randomString();
      admin.creator().topic(theTopic).numberOfPartitions(1).run().toCompletableFuture().join();
      try (var producer = Producer.of(SERVICE.bootstrapServers())) {
        var dummy = new byte[1024];
        IntStream.range(0, 10000)
            .mapToObj(i -> producer.send(Record.builder().topic(theTopic).value(dummy).build()))
            .collect(Collectors.toUnmodifiableSet())
            .forEach(i -> i.toCompletableFuture().join());
      }

      // request a plan
      var request = new BalancerHandler.BalancerPostRequest();
      request.balancerConfig =
          Map.of(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, Pattern.quote(theTopic));
      var theReport = submitPlanGeneration(handler, request);
      Assertions.assertEquals(Searched, theReport.phase, "Plan is ready");

      // now trigger ongoing migration
      admin
          .moveToBrokers(Map.of(TopicPartition.of(theTopic, 0), List.of(0, 1, 2)))
          .toCompletableFuture()
          .join();

      handler
          .put(httpRequest(Map.of("id", theReport.id, "executor", NoOpExecutor.class.getName())))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      var progress1 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(theReport.id)).toCompletableFuture().join());
      Assertions.assertEquals(ExecutionFailed, progress1.phase, "Ongoing Migration");
      Assertions.assertNotNull(progress1.exception);
    }
    SERVICE.close();
  }

  @Test
  @Timeout(value = 60)
  void testGenerationDetectOngoing() {
    var base =
        ClusterInfo.builder()
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
        ClusterInfo.builder(base)
            .mapLog(r -> Replica.builder(r).isFuture(iter0.next()).build())
            .build();
    var clusterHasAdding =
        ClusterInfo.builder(base)
            .mapLog(r -> Replica.builder(r).isAdding(iter1.next()).build())
            .build();
    var clusterHasRemoving =
        ClusterInfo.builder(base)
            .mapLog(r -> Replica.builder(r).isRemoving(iter2.next()).build())
            .build();
    var admin = Mockito.mock(Admin.class);
    Mockito.when(admin.brokers())
        .thenAnswer(invoke -> CompletableFuture.completedFuture(List.of()));
    Mockito.when(admin.topicNames(Mockito.anyBoolean()))
        .thenAnswer(invoke -> CompletableFuture.completedFuture(Set.of("A", "B", "C")));
    Mockito.when(admin.clusterInfo(Mockito.any()))
        .thenAnswer(invoke -> CompletableFuture.completedFuture(clusterHasFuture));
    try (var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      var task0 =
          (BalancerHandler.PostPlanResponse)
              handler.post(defaultPostPlan).toCompletableFuture().join();
      Utils.waitFor(
          () ->
              ((BalancerHandler.PlanExecutionProgress)
                          handler.get(Channel.ofTarget(task0.id)).toCompletableFuture().join())
                      .phase
                  == SearchFailed,
          Duration.ofSeconds(5));

      Mockito.when(admin.clusterInfo(Mockito.any()))
          .thenAnswer((invoke) -> CompletableFuture.completedFuture(clusterHasAdding));
      var task1 =
          (BalancerHandler.PostPlanResponse)
              handler.post(defaultPostPlan).toCompletableFuture().join();
      Utils.waitFor(
          () ->
              ((BalancerHandler.PlanExecutionProgress)
                          handler.get(Channel.ofTarget(task1.id)).toCompletableFuture().join())
                      .phase
                  == SearchFailed,
          Duration.ofSeconds(5));

      Mockito.when(admin.clusterInfo(Mockito.any()))
          .thenAnswer((invoke) -> CompletableFuture.completedFuture(clusterHasRemoving));
      var task2 =
          (BalancerHandler.PostPlanResponse)
              handler.post(defaultPostPlan).toCompletableFuture().join();
      Utils.waitFor(
          () ->
              ((BalancerHandler.PlanExecutionProgress)
                          handler.get(Channel.ofTarget(task2.id)).toCompletableFuture().join())
                      .phase
                  == SearchFailed,
          Duration.ofSeconds(5));
    }
  }

  @Test
  @Timeout(value = 60)
  void testPutSanityCheck() {
    var topic = createAndProduceTopic(1, SERVICE).iterator().next();
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      var request = new BalancerHandler.BalancerPostRequest();
      request.balancerConfig =
          Map.of(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, Pattern.quote(topic));
      var theProgress = submitPlanGeneration(handler, request);

      // pick a partition and alter its placement
      var theChange = theProgress.plan.changes.stream().findAny().orElseThrow();
      admin
          .moveToBrokers(
              Map.of(TopicPartition.of(theChange.topic, theChange.partition), List.of(0, 1, 2)))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(10));

      // assert
      handler
          .put(httpRequest(Map.of("id", theProgress.id, "executor", NoOpExecutor.class.getName())))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(5));

      var result =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(theProgress.id)).toCompletableFuture().join());
      Assertions.assertEquals(
          ExecutionFailed,
          result.phase,
          "The cluster state has changed, prevent the plan from execution");
      Assertions.assertNotNull(
          result.exception, "The cluster state has changed, prevent the plan from execution");
    }
    SERVICE.close();
  }

  @Test
  @Timeout(value = 60)
  void testLookupRebalanceProgress() {
    createAndProduceTopic(3, SERVICE);
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      var progress = submitPlanGeneration(handler, new BalancerPostRequest());
      Assertions.assertEquals(Searched, progress.phase);

      // not scheduled yet
      Utils.sleep(Duration.ofSeconds(1));
      var progress0 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(progress.id)).toCompletableFuture().join());
      Assertions.assertEquals(progress.id, progress0.id);
      Assertions.assertEquals(Searched, progress0.phase);
      Assertions.assertNull(progress0.exception);

      // schedule
      var response =
          Assertions.assertInstanceOf(
              BalancerHandler.PutPlanResponse.class,
              handler
                  .put(
                      httpRequest(
                          Map.of("id", progress.id, "executor", LatchExecutor.class.getName())))
                  .toCompletableFuture()
                  .join());
      Assertions.assertNotNull(response.id, "The plan should be executed");

      // not done yet
      Utils.sleep(Duration.ofSeconds(1));
      var progress1 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(response.id)).toCompletableFuture().join());
      Assertions.assertEquals(progress.id, progress1.id);
      Assertions.assertEquals(Executing, progress1.phase);
      Assertions.assertNull(progress1.exception);

      // it is done
      LatchExecutor.latch.countDown();
      Utils.sleep(Duration.ofSeconds(1));
      var progress2 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(response.id)).toCompletableFuture().join());
      Assertions.assertEquals(progress.id, progress2.id);
      Assertions.assertEquals(Executed, progress2.phase);
      Assertions.assertNull(progress2.exception);
    }
  }

  @Test
  @Timeout(value = 60)
  void testLookupBadExecutionProgress() {
    createAndProduceTopic(3, SERVICE);
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      var post =
          Assertions.assertInstanceOf(
              BalancerHandler.PostPlanResponse.class,
              handler
                  .post(httpRequest(Map.of(CLUSTER_COSTS_KEY, defaultDecreasing)))
                  .toCompletableFuture()
                  .join());
      Utils.waitFor(
          () ->
              ((BalancerHandler.PlanExecutionProgress)
                          handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join())
                      .phase
                  == Searched);
      var generated =
          ((BalancerHandler.PlanExecutionProgress)
                      handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join())
                  .phase
              == Searched;
      Assertions.assertTrue(generated, "The plan should be generated");

      var progress0 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join());
      Assertions.assertEquals(Searched, progress0.phase, "The plan is ready");

      // schedule
      handler.put(
          httpRequest(Map.of("id", post.id, "executor", ExceptionExecutor.class.getName())));
      // var response =
      //     Assertions.assertInstanceOf(
      //         BalancerHandler.PutPlanResponse.class,
      //         handler.put(httpRequest(Map.of("id", post.id))).toCompletableFuture().join());
      // Assertions.assertNotNull(response.id, "The execution scheduled");

      // exception
      Utils.sleep(Duration.ofSeconds(1));
      var progress =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join());
      Assertions.assertEquals(post.id, progress.id);
      Assertions.assertEquals(ExecutionFailed, progress.phase);
      Assertions.assertNotNull(progress.exception);
      Assertions.assertInstanceOf(String.class, progress.exception);
    }
  }

  @Test
  @Timeout(value = 60)
  void testBadLookupRequest() {
    createAndProduceTopic(3, SERVICE);
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
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
    var topics = createAndProduceTopic(3, SERVICE);
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      var request = new BalancerHandler.BalancerPostRequest();
      request.balancerConfig =
          Map.of(
              BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX,
              topics.stream().map(Pattern::quote).collect(Collectors.joining("|", "(", ")")));
      var progress = submitPlanGeneration(handler, request);

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
                    .anyMatch(replica -> replica.isFuture() || !replica.isSync()));
      } catch (Exception ignore) {
      }

      Assertions.assertDoesNotThrow(
          () -> handler.put(httpRequest(Map.of("id", progress.id))).toCompletableFuture().join(),
          "Idempotent behavior");
    }
  }

  @Test
  void testParsePostRequest() {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      // create a topic to avoid empty cluster
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(3)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      var clusterInfo =
          admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();
      // no cost weight
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> BalancerHandler.parsePostRequestWrapper(new BalancerPostRequest(), clusterInfo),
          "clusterCosts must be specified");
      {
        // minimal
        var request = new BalancerPostRequest();
        request.clusterCosts = List.of(costWeight(DecreasingCost.class.getName(), 1));
        var postRequest = BalancerHandler.parsePostRequestWrapper(request, clusterInfo);
        var config = postRequest.algorithmConfig;
        Assertions.assertInstanceOf(HasClusterCost.class, config.clusterCostFunction());
        Assertions.assertTrue(config.clusterCostFunction().toString().contains("DecreasingCost"));
        Assertions.assertTrue(config.clusterCostFunction().toString().contains("weight 1"));
        Assertions.assertEquals(TIMEOUT_DEFAULT, postRequest.algorithmConfig.timeout().toSeconds());
      }
      {
        // use custom filter/timeout/balancer config/cost function
        var randomTopic0 = Utils.randomString();
        var randomTopic1 = Utils.randomString();
        var request = new BalancerPostRequest();
        request.timeout = Duration.ofSeconds(32);
        request.balancerConfig = Map.of("KEY", "VALUE");
        request.clusterCosts = List.of(costWeight(DecreasingCost.class.getName(), 1));

        var postRequest = BalancerHandler.parsePostRequestWrapper(request, clusterInfo);
        var config = postRequest.algorithmConfig;
        Assertions.assertInstanceOf(HasClusterCost.class, config.clusterCostFunction());
        Assertions.assertEquals(
            1.0, config.clusterCostFunction().clusterCost(clusterInfo, ClusterBean.EMPTY).value());
        Assertions.assertEquals(
            1.0, config.clusterCostFunction().clusterCost(clusterInfo, ClusterBean.EMPTY).value());
        Assertions.assertEquals(
            1.0, config.clusterCostFunction().clusterCost(clusterInfo, ClusterBean.EMPTY).value());
        Assertions.assertEquals(32, postRequest.algorithmConfig.timeout().toSeconds());
      }
      {
        // malformed content
        var balancerRequest3 = new BalancerPostRequest();
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> BalancerHandler.parsePostRequestWrapper(balancerRequest3, clusterInfo),
            "Negative timeout");

        var balancerRequest4 = new BalancerPostRequest();
        var costWeight = costWeight("yes", 1);
        balancerRequest4.clusterCosts = List.of(costWeight);
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> BalancerHandler.parsePostRequestWrapper(balancerRequest4, clusterInfo),
            "Malformed cost weight");
      }
    }
  }

  @Test
  void testTimeout() {
    createAndProduceTopic(5, SERVICE);
    var costFunction = Collections.singleton(costWeight(TimeoutCost.class.getName(), 1));
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      var channel = httpRequest(Map.of(TIMEOUT_KEY, "10", CLUSTER_COSTS_KEY, costFunction));
      var post =
          (BalancerHandler.PostPlanResponse) handler.post(channel).toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(11));

      var progress =
          (BalancerHandler.PlanExecutionProgress)
              handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join();
      Assertions.assertEquals(SearchFailed, progress.phase);
      Assertions.assertNotNull(
          progress.exception, "The generation timeout and failed with some reason");
    }
  }

  @Test
  void testCostWithSensor() {
    var topics = createAndProduceTopic(3, SERVICE);
    var function = List.of(costWeight(SensorAndCost.class.getName(), 1));
    var functionName = function.stream().map(x -> x.cost).collect(Collectors.toUnmodifiableSet());
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, functionName))) {
      var invoked = new AtomicBoolean();
      SensorAndCost.callback.set(
          (clusterBean) -> {
            var metrics =
                clusterBean.all().values().stream()
                    .flatMap(Collection::stream)
                    .filter(x -> x instanceof JvmMemory)
                    .collect(Collectors.toUnmodifiableSet());
            if (metrics.size() < 3)
              throw new NoSufficientMetricsException(
                  new SensorAndCost(null), Duration.ofSeconds(3));
            metrics.forEach(i -> Assertions.assertInstanceOf(JvmMemory.class, i));
            invoked.set(true);
          });

      var request = new BalancerHandler.BalancerPostRequest();
      request.timeout = Duration.ofSeconds(15);
      request.clusterCosts = function;
      request.balancerConfig =
          Map.of(
              BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX,
              topics.stream().map(Pattern::quote).collect(Collectors.joining("|", "(", ")")));
      var progress = submitPlanGeneration(handler, request);

      Assertions.assertEquals(Searched, progress.phase);
      Assertions.assertTrue(invoked.get());
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
            .toList();
    var destPlacement =
        IntStream.range(0, 10)
            .mapToObj(partition -> Map.entry(ThreadLocalRandom.current().nextInt(), partition))
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();
    var base =
        ClusterInfo.builder()
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
        ClusterInfo.builder(base)
            .addTopic(
                "Pipeline",
                1,
                (short) 10,
                r ->
                    Replica.builder(r)
                        .broker(base.node(srcIter.next()))
                        .isPreferredLeader(srcPrefIter.next())
                        .path(srcDirIter.next())
                        .build())
            .build();
    var dstIter = destPlacement.iterator();
    var dstPrefIter = Stream.iterate(true, (ignore) -> false).iterator();
    var dstDirIter = Stream.generate(() -> "/folder1").iterator();
    var destCluster =
        ClusterInfo.builder(base)
            .addTopic(
                "Pipeline",
                1,
                (short) 10,
                r ->
                    Replica.builder(r)
                        .broker(base.node(dstIter.next()))
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
                ClusterInfo.builder(base).addTopic("Pipeline", 1, (short) 3).build().replicas(),
                ClusterInfo.builder(base).addTopic("Pipeline", 5, (short) 3).build().replicas()),
        "Should be a replica list");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            BalancerHandler.Change.from(
                ClusterInfo.builder(base).addTopic("Pipeline", 5, (short) 3).build().replicas(),
                ClusterInfo.builder(base).addTopic("Pipeline", 1, (short) 3).build().replicas()),
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

  @Test
  void testExecutorConfig() {
    var topic = createAndProduceTopic(1, SERVICE).iterator().next();
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      var request = new BalancerHandler.BalancerPostRequest();
      request.balancerConfig = Map.of(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, topic);
      var theProgress = submitPlanGeneration(handler, request);

      var value0 = Utils.randomString();
      var value1 = Utils.randomString();
      var value2 = Utils.randomString();
      handler
          .put(
              httpRequest(
                  Map.of(
                      "id",
                      theProgress.id,
                      "executor",
                      ExecutorWrapper.class.getName(),
                      "executorConfig",
                      Map.of("value0", value0, "value1", value1, "value2", value2))))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(3));

      var config = ExecutorWrapper.configWrapper.get();
      Assertions.assertNotNull(config);
      Assertions.assertEquals(value0, config.requireString("value0"));
      Assertions.assertEquals(value1, config.requireString("value1"));
      Assertions.assertEquals(value2, config.requireString("value2"));
    }
  }

  @Test
  void testBalancerConfig() {
    createAndProduceTopic(1, SERVICE);
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var handler = new BalancerHandler(admin, metricStore(admin, Set.of()))) {
      var request = new BalancerPostRequest();
      request.balancer = SpyBalancer.class.getName();
      request.balancerConfig =
          Map.ofEntries(
              Map.entry("key0", "value0"),
              Map.entry("key1", "value1"),
              Map.entry("key2", "value2"));

      // register callback to retrieve the config detail
      var acceptedConfig = new AtomicReference<AlgorithmConfig>();
      SpyBalancer.offerCallbacks.add(acceptedConfig::set);

      // submit plan
      var theProgress = submitPlanGeneration(handler, request);
      Assertions.assertEquals(Searched, theProgress.phase);

      // ensure the configs are there
      Assertions.assertEquals(
          Map.ofEntries(
              Map.entry("key0", "value0"),
              Map.entry("key1", "value1"),
              Map.entry("key2", "value2")),
          acceptedConfig.get().balancerConfig().raw(),
          "The specified config has been propagated to the balancer");
    }
  }

  /** Submit the plan and wait until it generated. */
  private BalancerHandler.PlanExecutionProgress submitPlanGeneration(
      BalancerHandler handler, BalancerPostRequest request) {
    if (request.clusterCosts.isEmpty()) request.clusterCosts = defaultDecreasing;
    var post =
        (BalancerHandler.PostPlanResponse)
            handler
                .post(Channel.ofRequest(JsonConverter.defaultConverter().toJson(request)))
                .toCompletableFuture()
                .join();
    Utils.waitFor(
        () -> {
          var progress =
              (BalancerHandler.PlanExecutionProgress)
                  handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join();
          Assertions.assertNull(progress.exception, progress.exception);
          return progress.phase == Searched;
        },
        Duration.ofSeconds(30));
    return (BalancerHandler.PlanExecutionProgress)
        handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join();
  }

  private static class NoOpExecutor implements RebalancePlanExecutor {

    private final LongAdder executionCounter = new LongAdder();

    public NoOpExecutor(Configuration configuration) {}

    @Override
    public CompletionStage<Void> run(Admin admin, ClusterInfo targetAllocation, Duration timeout) {
      executionCounter.increment();
      return CompletableFuture.completedFuture(null);
    }

    int count() {
      return executionCounter.intValue();
    }
  }

  public static class DecreasingCost implements HasClusterCost {

    private ClusterInfo original;

    public DecreasingCost(Configuration configuration) {}

    private double value0 = 1.0;

    @Override
    public synchronized ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
      if (original == null) original = clusterInfo;
      if (ClusterInfo.findNonFulfilledAllocation(original, clusterInfo).isEmpty()) return () -> 1;
      double theCost = value0;
      value0 = value0 * 0.998;
      return () -> theCost;
    }
  }

  public static class IncreasingCost implements HasClusterCost {

    private ClusterInfo original;

    public IncreasingCost(Configuration configuration) {}

    private double value0 = 1.0;

    @Override
    public synchronized ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
      if (original == null) original = clusterInfo;
      if (ClusterInfo.findNonFulfilledAllocation(original, clusterInfo).isEmpty()) return () -> 1;
      double theCost = value0;
      value0 = value0 * 1.002;
      return () -> theCost;
    }
  }

  public static class SensorAndCost extends DecreasingCost {

    static AtomicReference<Consumer<ClusterBean>> callback = new AtomicReference<>();

    public SensorAndCost(Configuration configuration) {
      super(configuration);
    }

    @Override
    public MetricSensor metricSensor() {
      return (c, ignored) -> List.of(HostMetrics.jvmMemory(c));
    }

    @Override
    public synchronized ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
      callback.get().accept(clusterBean);
      return super.clusterCost(clusterInfo, clusterBean);
    }
  }

  public static class TimeoutCost implements HasClusterCost {
    @Override
    public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
      throw new NoSufficientMetricsException(this, Duration.ofSeconds(10));
    }
  }

  public static class SpyBalancer extends SingleStepBalancer {

    public static List<Consumer<AlgorithmConfig>> offerCallbacks =
        Collections.synchronizedList(new ArrayList<>());

    @Override
    public Optional<Plan> offer(AlgorithmConfig config) {
      offerCallbacks.forEach(c -> c.accept(config));
      offerCallbacks.clear();
      return super.offer(config);
    }
  }

  public static class LatchExecutor extends NoOpExecutor {
    static final CountDownLatch latch = new CountDownLatch(1);

    public LatchExecutor(Configuration configuration) {
      super(configuration);
    }

    @Override
    public CompletionStage<Void> run(Admin admin, ClusterInfo targetAllocation, Duration timeout) {
      return super.run(admin, targetAllocation, Duration.ofSeconds(5))
          // Use another thread to block this completion to avoid deadlock in
          // BalancerHandler#put
          .thenApplyAsync(
              i -> {
                Utils.packException(() -> latch.await());
                return i;
              });
    }
  }

  public static class ExceptionExecutor extends NoOpExecutor {
    public ExceptionExecutor(Configuration configuration) {
      super(configuration);
    }

    @Override
    public CompletionStage<Void> run(Admin admin, ClusterInfo targetAllocation, Duration timeout) {
      return super.run(admin, targetAllocation, Duration.ofSeconds(5))
          .thenCompose(ignored -> CompletableFuture.failedFuture(new RuntimeException("Boom")));
    }
  }

  public static class ExecutorWrapper extends NoOpExecutor {

    static AtomicReference<Configuration> configWrapper = new AtomicReference<>();

    public ExecutorWrapper(Configuration configuration) {
      super(configuration);
      configWrapper.set(configuration);
    }
  }

  private static BalancerProblemFormat.CostWeight costWeight(String cost, double weight) {
    var cw = new BalancerProblemFormat.CostWeight();
    cw.cost = cost;
    cw.weight = weight;
    return cw;
  }

  private static Channel httpRequest(Map<String, ?> payload) {
    return Channel.ofRequest(JsonConverter.defaultConverter().toJson(payload));
  }

  @Test
  void testJsonToBalancerPostRequest() {
    var json =
        "{\"balancer\":\"org.astraea.common.balancer.algorithms.GreedyBalancer\""
            + ", \"clusterCosts\":[{\"cost\":\"aaa\"}],"
            + "\"balancerConfig\":{"
            + "    \"balancer.allowed.topics.regex\": \"regex.....\""
            + "  },"
            + "\"moveCosts\":["
            + "    \"org.astraea.common.cost.RecordSizeCost\","
            + "    \"org.astraea.common.cost.ReplicaLeaderCost\""
            + "  ],"
            + "  \"costConfig\":"
            + "  {"
            + "    \"maxMigratedSize\": \"500MB\","
            + "    \"maxMigratedLeader\": \"50\""
            + "  }"
            + "}";
    var request =
        JsonConverter.defaultConverter().fromJson(json, TypeRef.of(BalancerPostRequest.class));
    Assertions.assertTrue(request.moveCosts.contains("org.astraea.common.cost.RecordSizeCost"));
    Assertions.assertTrue(request.moveCosts.contains("org.astraea.common.cost.ReplicaLeaderCost"));
    Assertions.assertEquals(
        "org.astraea.common.balancer.algorithms.GreedyBalancer", request.balancer);
    Assertions.assertNotNull(request.balancerConfig);
    Assertions.assertNotNull(request.timeout);
    Assertions.assertEquals(
        Map.of("balancer.allowed.topics.regex", "regex....."), request.balancerConfig);

    Assertions.assertEquals(1, request.clusterCosts.size());
    Assertions.assertEquals("aaa", request.clusterCosts.get(0).cost);
    Assertions.assertEquals(1D, request.clusterCosts.get(0).weight);

    Assertions.assertEquals(2, request.moveCosts.size());
    Assertions.assertEquals(2, request.costConfig.size());
    Assertions.assertEquals("500MB", request.costConfig.get("maxMigratedSize"));
    Assertions.assertEquals("50", request.costConfig.get("maxMigratedLeader"));

    var noCostRequest =
        JsonConverter.defaultConverter()
            .fromJson(
                "{\"balancer\":\"org.astraea.common.balancer.algorithms.GreedyBalancer\"}",
                TypeRef.of(BalancerPostRequest.class));

    Assertions.assertThrows(IllegalArgumentException.class, noCostRequest::parse);
  }

  private MetricStore metricStore(Admin admin, Set<String> costFunctions) {
    Function<Integer, Integer> brokerIdToJmxPort = (id) -> SERVICE.jmxServiceURL().getPort();
    Supplier<CompletionStage<Map<Integer, MBeanClient>>> clientSupplier =
        () ->
            admin
                .brokers()
                .thenApply(
                    brokers ->
                        brokers.stream()
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    Broker::id,
                                    b ->
                                        JndiClient.of(b.host(), brokerIdToJmxPort.apply(b.id())))));
    var cf = Utils.costFunctions(costFunctions, HasClusterCost.class, Configuration.EMPTY);
    var metricSensors = cf.stream().map(CostFunction::metricSensor).toList();
    return MetricStore.builder()
        .beanExpiration(Duration.ofMinutes(2))
        .receivers(List.of(MetricStore.Receiver.local(clientSupplier)))
        .sensorsSupplier(
            () ->
                metricSensors.stream()
                    .distinct()
                    .collect(
                        Collectors.toUnmodifiableMap(
                            Function.identity(), ignored -> (id, ee) -> {})))
        .build();
  }
}
