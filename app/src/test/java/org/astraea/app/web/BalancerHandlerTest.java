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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.executor.RebalanceAdmin;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
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
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

public class BalancerHandlerTest extends RequireBrokerCluster {

  @Test
  void testReport() throws ExecutionException, InterruptedException {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler =
          new BalancerHandler(admin, MultiplicationCost.decreasing(), new ReplicaSizeCost());
      var report =
          submitPlanGeneration(handler, Channel.ofQueries(Map.of(BalancerHandler.LOOP_KEY, "3000")))
              .report;
      Assertions.assertNotNull(report.id);
      Assertions.assertEquals(3000, report.limit);
      Assertions.assertNotEquals(0, report.changes.size());
      Assertions.assertTrue(report.cost >= report.newCost);
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
  void testTopic() throws ExecutionException, InterruptedException {
    var topicNames = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler =
          new BalancerHandler(admin, MultiplicationCost.decreasing(), new ReplicaSizeCost());
      var report =
          submitPlanGeneration(
                  handler,
                  Channel.ofQueries(
                      Map.of(
                          BalancerHandler.LOOP_KEY,
                          "30",
                          BalancerHandler.TOPICS_KEY,
                          topicNames.get(0))))
              .report;
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
  void testTopics() throws ExecutionException, InterruptedException {
    var topicNames = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler =
          new BalancerHandler(admin, MultiplicationCost.decreasing(), new ReplicaSizeCost());
      var report =
          submitPlanGeneration(
                  handler,
                  Channel.ofQueries(
                      Map.of(
                          BalancerHandler.LOOP_KEY,
                          "30",
                          BalancerHandler.TOPICS_KEY,
                          topicNames.get(0) + "," + topicNames.get(1))))
              .report;
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
  void testNoReport() throws ExecutionException, InterruptedException {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      Utils.sleep(Duration.ofSeconds(1));
      var handler =
          new BalancerHandler(admin, MultiplicationCost.increasing(), new ReplicaSizeCost());
      var post =
          Assertions.assertInstanceOf(
              BalancerHandler.PostPlanResponse.class,
              handler
                  .post(Channel.ofQueries(Map.of(BalancerHandler.LOOP_KEY, "10")))
                  .toCompletableFuture()
                  .get());
      Utils.sleep(Duration.ofSeconds(5));
      var progress =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(post.id)).toCompletableFuture().get());
      Assertions.assertNotNull(post.id);
      Assertions.assertEquals(post.id, progress.id);
      Assertions.assertFalse(progress.generated);
      Assertions.assertNotNull(progress.exception);
    }
  }

  @Test
  void testPut() throws ExecutionException, InterruptedException {
    // arrange
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor = new NoOpExecutor();
      var handler =
          new BalancerHandler(
              admin,
              MultiplicationCost.decreasing(),
              new ReplicaSizeCost(),
              RebalancePlanGenerator.random(30),
              theExecutor);
      var progress =
          submitPlanGeneration(handler, Channel.ofQueries(Map.of(BalancerHandler.LOOP_KEY, "100")));
      var thePlanId = progress.id;

      // act
      var response =
          Assertions.assertInstanceOf(
              BalancerHandler.PutPlanResponse.class,
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", thePlanId))))
                  .toCompletableFuture()
                  .get());
      Utils.sleep(Duration.ofSeconds(1));

      // assert
      Assertions.assertEquals(Response.ACCEPT.code(), response.code());
      Assertions.assertEquals(thePlanId, response.id);
      Assertions.assertEquals(1, theExecutor.count());
    }
  }

  @Test
  void testBadPut() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler =
          new BalancerHandler(
              admin,
              MultiplicationCost.decreasing(),
              new ReplicaSizeCost(),
              RebalancePlanGenerator.random(30),
              new NoOpExecutor());

      // no id offered
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> handler.put(Channel.EMPTY).toCompletableFuture().get(),
          "The 'id' field is required");

      // no such plan id
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", "no such plan"))))
                  .toCompletableFuture()
                  .get(),
          "The requested plan doesn't exists");
    }
  }

  @RepeatedTest(value = 10)
  void testSubmitRebalancePlanThreadSafe() throws ExecutionException, InterruptedException {
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor = new NoOpExecutor();
      var handler =
          new BalancerHandler(
              admin,
              MultiplicationCost.decreasing(),
              new ReplicaSizeCost(),
              RebalancePlanGenerator.random(30),
              theExecutor);
      var progress = submitPlanGeneration(handler, Channel.EMPTY);

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
                            Channel.ofRequest(PostRequest.of(Map.of("id", progress.id)));
                        // use cyclic barrier to ensure all threads are ready to work
                        Utils.packException(() -> barrier.await());
                        // send the put request
                        Utils.packException(() -> handler.put(request).toCompletableFuture().get());
                      }));

      // await work done
      executor.shutdown();
      Assertions.assertTrue(
          Utils.packException(() -> executor.awaitTermination(3, TimeUnit.SECONDS)));

      // the rebalance task is triggered in async manner, it may take some time to getting schedule
      Utils.sleep(Duration.ofMillis(500));
      // test if the plan has been executed just once
      Assertions.assertEquals(1, theExecutor.count());
    }
  }

  @Test
  void testRebalanceOnePlanAtATime() throws ExecutionException, InterruptedException {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor =
          new NoOpExecutor() {
            @Override
            public void run(RebalanceAdmin rebalanceAdmin, ClusterLogAllocation targetAllocation) {
              super.run(rebalanceAdmin, targetAllocation);
              Utils.sleep(Duration.ofSeconds(10));
            }
          };
      var handler =
          new BalancerHandler(
              admin,
              MultiplicationCost.decreasing(),
              new ReplicaSizeCost(),
              RebalancePlanGenerator.random(30),
              theExecutor);
      var plan0 = submitPlanGeneration(handler, Channel.EMPTY);
      var plan1 = submitPlanGeneration(handler, Channel.EMPTY);

      Assertions.assertDoesNotThrow(
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", plan0.id))))
                  .toCompletableFuture()
                  .get());
      Assertions.assertThrows(
          IllegalStateException.class,
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", plan1.id))))
                  .toCompletableFuture()
                  .get());
    }
  }

  @Test
  void testRebalanceDetectOngoing() throws ExecutionException, InterruptedException {
    try (var admin = Admin.of(bootstrapServers())) {
      var theTopic = Utils.randomString();
      admin.creator().topic(theTopic).numberOfPartitions(1).run().toCompletableFuture().join();
      try (var producer = Producer.of(bootstrapServers())) {
        var dummy = new byte[1024];
        IntStream.range(0, 100000)
            .mapToObj(i -> producer.sender().topic(theTopic).value(dummy).run())
            .collect(Collectors.toUnmodifiableSet())
            .forEach(i -> i.toCompletableFuture().join());
      }

      var handler =
          new BalancerHandler(
              admin,
              MultiplicationCost.decreasing(),
              new ReplicaSizeCost(),
              RebalancePlanGenerator.random(30),
              new NoOpExecutor());
      var theReport =
          submitPlanGeneration(
              handler, Channel.ofQueries(Map.of(BalancerHandler.TOPICS_KEY, theTopic)));

      // create an ongoing reassignment
      Assertions.assertEquals(1, admin.replicas(Set.of(theTopic)).size());
      admin.migrator().partition(theTopic, 0).moveTo(List.of(0, 1, 2));

      // debounce wait
      for (int i = 0; i < 2; i++) {
        Utils.waitForNonNull(
            () -> !admin.addingReplicas(Set.of(theTopic)).isEmpty(),
            Duration.ofSeconds(10),
            Duration.ofMillis(10));
      }

      Assertions.assertThrows(
          IllegalStateException.class,
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", theReport.id))))
                  .toCompletableFuture()
                  .get());
    }
  }

  @Test
  void testPutSanityCheck() throws ExecutionException, InterruptedException {
    var topic = createAndProduceTopic(1).get(0);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor = new NoOpExecutor();
      var handler =
          new BalancerHandler(
              admin,
              MultiplicationCost.decreasing(),
              new ReplicaSizeCost(),
              RebalancePlanGenerator.random(30),
              theExecutor);
      var theReport =
          submitPlanGeneration(
                  handler, Channel.ofQueries(Map.of(BalancerHandler.TOPICS_KEY, topic)))
              .report;

      // pick a partition and alter its placement
      var theChange = theReport.changes.stream().findAny().orElseThrow();
      admin.migrator().partition(theChange.topic, theChange.partition).moveTo(List.of(0, 1, 2));
      Utils.sleep(Duration.ofSeconds(10));

      // assert
      Assertions.assertThrows(
          IllegalStateException.class,
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", theReport.id))))
                  .toCompletableFuture()
                  .get(),
          "The cluster state has changed, prevent the plan from execution");
    }
  }

  @Test
  void testLookupRebalanceProgress() throws ExecutionException, InterruptedException {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor =
          new NoOpExecutor() {
            final CountDownLatch latch = new CountDownLatch(1);

            @Override
            public void run(RebalanceAdmin rebalanceAdmin, ClusterLogAllocation targetAllocation) {
              super.run(rebalanceAdmin, targetAllocation);
              Utils.packException(() -> latch.await());
            }
          };
      var handler =
          new BalancerHandler(
              admin,
              MultiplicationCost.decreasing(),
              new ReplicaSizeCost(),
              RebalancePlanGenerator.random(30),
              theExecutor);
      var progress = submitPlanGeneration(handler, Channel.EMPTY);
      Assertions.assertTrue(progress.generated, "The plan should be generated");

      // not scheduled yet
      Utils.sleep(Duration.ofSeconds(1));
      var progress0 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(progress.id)).toCompletableFuture().get());
      Assertions.assertEquals(progress.id, progress0.id);
      Assertions.assertTrue(progress0.generated);
      Assertions.assertFalse(progress0.scheduled);
      Assertions.assertFalse(progress0.done);
      Assertions.assertNull(progress0.exception);

      // schedule
      var response =
          Assertions.assertInstanceOf(
              BalancerHandler.PutPlanResponse.class,
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", progress.id))))
                  .toCompletableFuture()
                  .get());
      Assertions.assertNotNull(response.id, "The plan should be executed");

      // not done yet
      Utils.sleep(Duration.ofSeconds(1));
      var progress1 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(response.id)).toCompletableFuture().get());
      Assertions.assertEquals(progress.id, progress1.id);
      Assertions.assertTrue(progress1.generated);
      Assertions.assertTrue(progress1.scheduled);
      Assertions.assertFalse(progress1.done);
      Assertions.assertNull(progress1.exception);

      // it is done
      theExecutor.latch.countDown();
      Utils.sleep(Duration.ofMillis(500));
      var progress2 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(response.id)).toCompletableFuture().get());
      Assertions.assertEquals(progress.id, progress2.id);
      Assertions.assertTrue(progress2.generated);
      Assertions.assertTrue(progress2.scheduled);
      Assertions.assertTrue(progress2.done);
      Assertions.assertNull(progress2.exception);
    }
  }

  @Test
  void testLookupBadExecutionProgress() throws ExecutionException, InterruptedException {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor =
          new NoOpExecutor() {
            @Override
            public void run(RebalanceAdmin rebalanceAdmin, ClusterLogAllocation targetAllocation) {
              super.run(rebalanceAdmin, targetAllocation);
              throw new RuntimeException("Boom");
            }
          };
      var handler =
          new BalancerHandler(
              admin,
              MultiplicationCost.decreasing(),
              new ReplicaSizeCost(),
              RebalancePlanGenerator.random(30),
              theExecutor);
      var post =
          Assertions.assertInstanceOf(
              BalancerHandler.PostPlanResponse.class,
              handler.post(Channel.EMPTY).toCompletableFuture().get());
      Utils.waitFor(
          () ->
              Utils.packException(
                      () ->
                          (BalancerHandler.PlanExecutionProgress)
                              handler.get(Channel.ofTarget(post.id)).toCompletableFuture().get())
                  .generated);
      var generated =
          ((BalancerHandler.PlanExecutionProgress)
                  handler.get(Channel.ofTarget(post.id)).toCompletableFuture().get())
              .generated;
      Assertions.assertTrue(generated, "The plan should be generated");

      // schedule
      var response =
          Assertions.assertInstanceOf(
              BalancerHandler.PutPlanResponse.class,
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", post.id))))
                  .toCompletableFuture()
                  .get());
      Assertions.assertNotNull(response.id, "The plan should be executed");

      // exception
      Utils.sleep(Duration.ofSeconds(1));
      var progress =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(response.id)).toCompletableFuture().get());
      Assertions.assertEquals(post.id, progress.id);
      Assertions.assertTrue(progress.generated);
      Assertions.assertTrue(progress.scheduled);
      Assertions.assertTrue(progress.done);
      Assertions.assertNotNull(progress.exception);
      Assertions.assertInstanceOf(String.class, progress.exception);
    }
  }

  @Test
  void testBadLookupRequest() throws ExecutionException, InterruptedException {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler =
          new BalancerHandler(
              admin,
              MultiplicationCost.decreasing(),
              new ReplicaSizeCost(),
              RebalancePlanGenerator.random(30),
              new NoOpExecutor());

      Assertions.assertEquals(
          404, handler.get(Channel.ofTarget("no such plan")).toCompletableFuture().get().code());

      // plan doesn't exists
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", "no such plan"))))
                  .toCompletableFuture()
                  .get(),
          "This plan doesn't exists");
    }
  }

  @Test
  void testPutIdempotent() throws ExecutionException, InterruptedException {
    var topics = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler =
          new BalancerHandler(
              admin,
              MultiplicationCost.decreasing(),
              new ReplicaSizeCost(),
              RebalancePlanGenerator.random(30),
              new StraightPlanExecutor());
      var progress =
          submitPlanGeneration(
              handler,
              Channel.ofQueries(Map.of(BalancerHandler.TOPICS_KEY, String.join(",", topics))));

      Assertions.assertDoesNotThrow(
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", progress.id))))
                  .toCompletableFuture()
                  .get(),
          "Schedule the rebalance task");

      // Wait until the migration occurred
      try {
        Utils.waitFor(
            () ->
                admin.replicas(Set.copyOf(topics)).stream()
                    .anyMatch(replica -> replica.isFuture() || !replica.inSync()));
      } catch (Exception ignore) {
      }

      Assertions.assertDoesNotThrow(
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", progress.id))))
                  .toCompletableFuture()
                  .get(),
          "Idempotent behavior");
    }
  }

  /** Submit the plan and wait until it generated. */
  private BalancerHandler.PlanExecutionProgress submitPlanGeneration(
      BalancerHandler handler, Channel channel) throws ExecutionException, InterruptedException {
    var post = (BalancerHandler.PostPlanResponse) handler.post(channel).toCompletableFuture().get();
    Utils.waitFor(
        () ->
            Utils.packException(
                    () ->
                        (BalancerHandler.PlanExecutionProgress)
                            handler.get(Channel.ofTarget(post.id)).toCompletableFuture().get())
                .generated);
    return (BalancerHandler.PlanExecutionProgress)
        handler.get(Channel.ofTarget(post.id)).toCompletableFuture().get();
  }

  private static class NoOpExecutor implements RebalancePlanExecutor {

    private final LongAdder executionCounter = new LongAdder();

    @Override
    public void run(RebalanceAdmin rebalanceAdmin, ClusterLogAllocation targetAllocation) {
      executionCounter.increment();
    }

    @Override
    public CompletionStage<Void> run(AsyncAdmin admin, ClusterLogAllocation targetAllocation) {
      executionCounter.increment();
      return null;
    }

    int count() {
      return executionCounter.intValue();
    }
  }
}
