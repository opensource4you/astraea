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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.metrics.ClusterBean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** A collection of helper methods that aid in verifying the implementation of balancer configs. */
public abstract class BalancerConfigTestSuite {

  private final Class<? extends Balancer> balancerClass;
  private final Configuration customConfig;

  public BalancerConfigTestSuite(Class<? extends Balancer> balancerClass, Configuration custom) {
    this.balancerClass = balancerClass;
    this.customConfig = custom;
  }

  @Test
  public void testUnsupportedConfigException() {
    final var balancer = Utils.construct(balancerClass, Configuration.EMPTY);
    final var cluster = cluster(20, 10, 10, (short) 5);
    final var testName =
        """
          Balancer implementation should raise an exception \
          when seeing an unsupported config with 'balancer.' prefix.
          """;

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            balancer.offer(
                AlgorithmConfig.builder()
                    .clusterInfo(cluster)
                    .clusterCost(decreasingCost())
                    .timeout(Duration.ofSeconds(2))
                    .configs(customConfig.raw())
                    .config("balancer.no.such.configuration", "oops")
                    .build()),
        testName);
  }

  @Test
  public void testBalancerAllowedTopicsRegex() {
    final var balancer = Utils.construct(balancerClass, Configuration.EMPTY);
    final var cluster = cluster(20, 10, 10, (short) 5);

    {
      var testName = "[test no limit]";
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  // This argument is not applied
                  // .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, regexRaw)
                  .build());
      AssertionsHelper.assertSomeMovement(cluster, plan.orElseThrow().proposal(), testName);
    }

    {
      var testName = "[test only allowed topics being altered]";
      var regexRaw =
          cluster.topicNames().stream()
              .limit(5)
              .map(Pattern::quote)
              .collect(Collectors.joining("|", "(", ")"));
      var regex = Pattern.compile(regexRaw);
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, regexRaw)
                  .build());
      AssertionsHelper.assertOnlyAllowedTopicMovement(
          cluster, plan.orElseThrow().proposal(), regex, testName);
    }

    {
      var testName = "[test the regex should match the whole topic name]";
      var regexRaw =
          cluster.topicNames().stream()
              .limit(5)
              .map(name -> name.substring(0, 1))
              .map(Pattern::quote)
              .collect(Collectors.joining("|", "(", ")"));
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, regexRaw)
                  .build());
      AssertionsHelper.assertNoMovement(cluster, plan.orElseThrow().proposal(), testName);
    }

    {
      var testName = "[test no allowed topic should generate no plan]";
      var regexRaw = "";
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, regexRaw)
                  .build());
      AssertionsHelper.assertNoMovement(cluster, plan.orElseThrow().proposal(), testName);
    }
  }

  @Test
  public void testBalancingMode() {
    final var balancer = Utils.construct(balancerClass, Configuration.EMPTY);
    final var cluster = cluster(10, 10, 10, (short) 5);

    {
      var testName = "[test all match]";
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_BROKER_BALANCING_MODE, "default:balancing")
                  .build());
      AssertionsHelper.assertSomeMovement(cluster, plan.orElseThrow().proposal(), testName);
    }

    {
      var testName = "[test no match]";
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_BROKER_BALANCING_MODE, "default:excluded")
                  .build());
      // since nothing can be moved. It is ok to return no plan.
      if (plan.isPresent()) {
        // But if we have a plan here. It must contain no movement.
        AssertionsHelper.assertNoMovement(cluster, plan.orElseThrow().proposal(), testName);
      }
    }

    {
      var testName = "[test some match]";
      var allowedBrokers = IntStream.range(1, 6).boxed().collect(Collectors.toUnmodifiableSet());
      var config =
          allowedBrokers.stream()
              .map(i -> i + ":balancing")
              .collect(Collectors.joining(",", "default:excluded,", ""));
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_BROKER_BALANCING_MODE, config)
                  .build());
      AssertionsHelper.assertOnlyAllowedBrokerMovement(
          cluster, plan.orElseThrow().proposal(), allowedBrokers::contains, testName);
    }
  }

  @Test
  public void testBalancingModeClear() {
    final var balancer = Utils.construct(balancerClass, Configuration.EMPTY);
    final var cluster = cluster(10, 30, 10, (short) 5);

    {
      var testName = "[test all clear]";
      Assertions.assertThrows(
          Exception.class,
          () ->
              balancer.offer(
                  AlgorithmConfig.builder()
                      .clusterInfo(cluster)
                      .clusterCost(decreasingCost())
                      .timeout(Duration.ofSeconds(2))
                      .configs(customConfig.raw())
                      .config(BalancerConfigs.BALANCER_BROKER_BALANCING_MODE, "default:clear")
                      .build()),
          testName);
    }

    {
      var testName = "[test some clear]";
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_BROKER_BALANCING_MODE, "0:clear,1:clear,2:clear")
                  .build());
      Assertions.assertTrue(plan.isPresent(), testName);
      var finalCluster = plan.get().proposal();
      Assertions.assertTrue(cluster.replicas().stream().anyMatch(x -> x.brokerId() == 0));
      Assertions.assertTrue(cluster.replicas().stream().anyMatch(x -> x.brokerId() == 1));
      Assertions.assertTrue(cluster.replicas().stream().anyMatch(x -> x.brokerId() == 2));
      Assertions.assertTrue(finalCluster.replicas().stream().noneMatch(x -> x.brokerId() == 0));
      Assertions.assertTrue(finalCluster.replicas().stream().noneMatch(x -> x.brokerId() == 1));
      Assertions.assertTrue(finalCluster.replicas().stream().noneMatch(x -> x.brokerId() == 2));
      AssertionsHelper.assertBrokerEmpty(
          finalCluster, (x) -> Set.of(0, 1, 2).contains(x), testName);
    }

    {
      var testName = "[test replication factor violation]";
      // 6 brokers, clear 3 brokers, remain 3 brokers, topic with replication factor 3 can fit this
      // cluster.
      var noViolatedCluster = cluster(6, 10, 10, (short) 3);
      Assertions.assertDoesNotThrow(
          () -> {
            var solution =
                balancer
                    .offer(
                        AlgorithmConfig.builder()
                            .clusterInfo(noViolatedCluster)
                            .clusterCost(decreasingCost())
                            .timeout(Duration.ofSeconds(2))
                            .configs(customConfig.raw())
                            .config(
                                BalancerConfigs.BALANCER_BROKER_BALANCING_MODE,
                                "0:clear,1:clear,2:clear")
                            .build())
                    .orElseThrow()
                    .proposal();
            AssertionsHelper.assertBrokerEmpty(
                solution, (x) -> Set.of(0, 1, 2).contains(x), testName);
          },
          testName);

      // 5 brokers, clear 3 brokers, remain 2 brokers, topic with replication factor 3 CANNOT fit
      // this cluster.
      var violatedCluster = cluster(5, 10, 10, (short) 3);
      Assertions.assertThrows(
          Exception.class,
          () ->
              balancer.offer(
                  AlgorithmConfig.builder()
                      .clusterInfo(violatedCluster)
                      .clusterCost(decreasingCost())
                      .timeout(Duration.ofSeconds(2))
                      .configs(customConfig.raw())
                      .config(
                          BalancerConfigs.BALANCER_BROKER_BALANCING_MODE, "0:clear,1:clear,2:clear")
                      .build()));
    }

    {
      var testName =
          "[test if allowed topics is used, disallowed partitions on cleared broker will be force to move]";
      var base =
          ClusterInfo.builder()
              .addNode(Set.of(1, 2, 3))
              .addFolders(
                  Map.ofEntries(
                      Map.entry(1, Set.of("/folder")),
                      Map.entry(2, Set.of("/folder")),
                      Map.entry(3, Set.of("/folder"))))
              .build();
      var node3 = base.node(3);
      var testCluster =
          ClusterInfo.builder(base)
              .addTopic("topic", 3, (short) 1)
              .addTopic("ok0", 10, (short) 1, r -> Replica.builder(r).brokerId(node3.id()).build())
              .addTopic("ok1", 10, (short) 1, r -> Replica.builder(r).brokerId(node3.id()).build())
              .addTopic("ok2", 10, (short) 1, r -> Replica.builder(r).brokerId(node3.id()).build())
              .build();

      var result =
          Assertions.assertDoesNotThrow(
              () ->
                  balancer.offer(
                      AlgorithmConfig.builder()
                          .clusterInfo(testCluster)
                          .clusterCost(decreasingCost())
                          .timeout(Duration.ofSeconds(2))
                          .configs(customConfig.raw())
                          // allow anything other than this topic
                          .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, "(?!topic).*")
                          // clear broker 3
                          .config(BalancerConfigs.BALANCER_BROKER_BALANCING_MODE, "3:clear")
                          // partition at broker 3 will be forced to move
                          .build()),
              testName);

      Assertions.assertTrue(result.isPresent());
      Assertions.assertNotEquals(
          List.of(),
          testCluster.replicas().stream().filter(x -> x.brokerId() == 3).toList(),
          "Originally, some replica located at broker 3");
      Assertions.assertEquals(
          List.of(),
          result.get().proposal().replicas().stream().filter(x -> x.brokerId() == 3).toList(),
          "Returned allocation has no replica located at broker 3");
      var toStay =
          testCluster.replicas().stream()
              .filter(x -> x.topic().equals("topic"))
              .filter(x -> x.brokerId() != 3)
              .collect(Collectors.toSet());
      Assertions.assertTrue(
          result.get().proposal().replicas().stream()
              .filter(x -> x.topic().equals("topic"))
              .collect(Collectors.toSet())
              .containsAll(toStay),
          "Disallowed partition stay still except those at broker 3");
    }

    {
      var testName = "[test if allowed brokers is used, disallowed broker won't be altered]";
      var solution =
          balancer
              .offer(
                  AlgorithmConfig.builder()
                      .clusterInfo(cluster)
                      .clusterCost(decreasingCost())
                      .timeout(Duration.ofSeconds(2))
                      .configs(customConfig.raw())
                      // clear broker 0
                      .config(
                          BalancerConfigs.BALANCER_BROKER_BALANCING_MODE,
                          "0:clear,"
                              +
                              // allow broker 1,2,3,4,5,6
                              "1:balancing,2:balancing,3:balancing,4:balancing,5:balancing,6:balancing,default:excluded")
                      // this will be ok since any replica at 0 can move to 1~6 without breaking
                      // replica factors
                      .build())
              .orElseThrow()
              .proposal();
      var before = cluster.topicPartitionReplicas();
      var after = solution.topicPartitionReplicas();
      var changed =
          after.stream()
              .filter(Predicate.not(before::contains))
              .collect(Collectors.toUnmodifiableSet());
      Assertions.assertTrue(after.stream().noneMatch(r -> r.brokerId() == 0), testName);
      Assertions.assertTrue(
          changed.stream().allMatch(r -> Set.of(1, 2, 3, 4, 5, 6).contains(r.brokerId())),
          testName);
    }

    {
      var testName =
          "[test if allowed brokers is used, insufficient allowed broker to fit replica factor requirement will raise an error]";
      Assertions.assertThrows(
          Exception.class,
          () ->
              balancer.offer(
                  AlgorithmConfig.builder()
                      .clusterInfo(cluster)
                      .clusterCost(decreasingCost())
                      .timeout(Duration.ofSeconds(2))
                      .configs(customConfig.raw())
                      // clear broker 0, allow broker 1
                      .config(
                          BalancerConfigs.BALANCER_BROKER_BALANCING_MODE,
                          "0:clear,1:balancing,default:excluded")
                      // this will raise an error if a partition has replicas at both 0 and 1. In
                      // this case, there is no allowed broker to adopt replica from 0, since the
                      // only allowed broker already has one replica on it. we cannot assign two
                      // replicas to one broker.
                      .build()),
          testName);
    }

    {
      var testName = "[if replica on clear broker is adding/removing/future, raise an exception]";
      var adding =
          ClusterInfo.builder(cluster)
              .mapLog(r -> r.brokerId() != 0 ? r : Replica.builder(r).isAdding(true).build())
              .build();
      var removing =
          ClusterInfo.builder(cluster)
              .mapLog(r -> r.brokerId() != 0 ? r : Replica.builder(r).isRemoving(true).build())
              .build();
      var future =
          ClusterInfo.builder(cluster)
              .mapLog(r -> r.brokerId() != 0 ? r : Replica.builder(r).isFuture(true).build())
              .build();
      for (var cc : List.of(adding, removing, future)) {
        Assertions.assertThrows(
            Exception.class,
            () ->
                balancer.offer(
                    AlgorithmConfig.builder()
                        .clusterInfo(cc)
                        .clusterCost(decreasingCost())
                        .timeout(Duration.ofSeconds(1))
                        .configs(customConfig.raw())
                        // clear broker 0 allow broker 1,2,3,4,5,6
                        .config(
                            BalancerConfigs.BALANCER_BROKER_BALANCING_MODE,
                            "0:clear,"
                                + "1:balancing,2:balancing,3:balancing,4:balancing,5:balancing,6:balancing")
                        .build()),
            testName);
      }
      for (var cc : List.of(adding, removing, future)) {
        Assertions.assertDoesNotThrow(
            () ->
                balancer.offer(
                    AlgorithmConfig.builder()
                        .clusterInfo(cc)
                        .clusterCost(decreasingCost())
                        .timeout(Duration.ofSeconds(1))
                        .configs(customConfig.raw())
                        // clear broker 1 allow broker 0,2,3,4,5,6,7
                        .config(
                            BalancerConfigs.BALANCER_BROKER_BALANCING_MODE,
                            "1:clear,"
                                + "0:balancing,2:balancing,3:balancing,4:balancing,5:balancing,6:balancing,"
                                + "7:balancing,default:excluded")
                        // adding/removing/future at 0 not 1, unrelated so no error
                        .build()),
            testName);
      }
    }

    {
      // Some balancer implementations have such logic flaw:
      // 1. The initial state[A] cannot be solution.
      // 2. There are brokers that need to be cleared.
      // 3. The load on those brokers been redistributed to other brokers. Creating the start
      //    state[B] for the solution search.
      // 4. The start state[B] solution is actually the best solution.
      // 5. Balancer think the start state[B] is the initial state[A]. And cannot be a solution(as
      // mentioned in 1).
      // 6. In fact, the start state[B] doesn't equal to the initial state[A]. Since there is a
      //    clearing work performed at step 3.
      // 7. Balancer cannot find any solution that is better than the start state(4) and therefore
      //    returns no solution.
      var testName =
          "[If the cluster after clear is the best solution, balancer should be able to return it]";
      var testCluster =
          ClusterInfo.builder()
              .addNode(Set.of(1, 2))
              .addFolders(
                  Map.ofEntries(Map.entry(1, Set.of("/folder")), Map.entry(2, Set.of("/folder"))))
              .addTopic("topic", 100, (short) 1)
              .build();
      Assertions.assertNotEquals(
          Optional.empty(),
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(testCluster)
                  .clusterBean(ClusterBean.EMPTY)
                  .clusterCost(new ReplicaLeaderCost())
                  .config(BalancerConfigs.BALANCER_BROKER_BALANCING_MODE, "1:clear")
                  .timeout(Duration.ofSeconds(2))
                  .build()),
          testName);
    }
  }

  private static ClusterInfo cluster(int nodes, int topics, int partitions, short replicas) {
    var builder =
        ClusterInfo.builder()
            .addNode(IntStream.range(0, nodes).boxed().collect(Collectors.toSet()))
            .addFolders(
                IntStream.range(0, nodes)
                    .boxed()
                    .collect(
                        Collectors.toMap(
                            id -> id, id -> Set.of("/folder0", "/folder1", "/folder2"))));
    for (int i = 0; i < topics; i++)
      builder = builder.addTopic(Utils.randomString(), partitions, replicas);
    return builder.build();
  }

  private static HasClusterCost decreasingCost() {
    return new HasClusterCost() {

      private final AtomicReference<ClusterInfo> initial = new AtomicReference<>();
      private final AtomicReference<Double> score = new AtomicReference<>(1.0);

      @Override
      public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
        if (initial.get() == null) initial.set(clusterInfo);
        return ClusterCost.of(
            clusterInfo == initial.get() ? 1 : score.updateAndGet(i -> i * 0.999999),
            () -> "DecreasingCost");
      }
    };
  }

  private static class AssertionsHelper {
    static void assertSomeMovement(ClusterInfo source, ClusterInfo target, String name) {
      Assertions.assertNotEquals(
          Set.of(),
          ClusterInfo.findNonFulfilledAllocation(source, target),
          name + ": Should have movements");
    }

    static void assertNoMovement(ClusterInfo source, ClusterInfo target, String name) {
      Assertions.assertEquals(
          Set.of(),
          ClusterInfo.findNonFulfilledAllocation(source, target),
          name + ": Should have no movement");
    }

    static void assertOnlyAllowedTopicMovement(
        ClusterInfo source, ClusterInfo target, Pattern allowedTopic, String name) {
      assertSomeMovement(source, target, name);
      Assertions.assertEquals(
          Set.of(),
          ClusterInfo.findNonFulfilledAllocation(source, target).stream()
              .filter(Predicate.not((tp) -> allowedTopic.asMatchPredicate().test(tp.topic())))
              .collect(Collectors.toUnmodifiableSet()),
          name + ": Only allowed topics been altered.");
    }

    static void assertOnlyAllowedBrokerMovement(
        ClusterInfo source, ClusterInfo target, Predicate<Integer> allowedBroker, String name) {
      assertSomeMovement(source, target, name);
      source
          .replicaStream()
          // for those replicas that are not allowed to move
          .filter(r -> !allowedBroker.test(r.brokerId()))
          // they should exist as-is in the target allocation
          .forEach(
              fixedReplica -> {
                target
                    .replicaStream()
                    .filter(targetReplica -> targetReplica.equals(fixedReplica))
                    .findFirst()
                    .ifPresentOrElse(
                        (r) -> {},
                        () -> {
                          Assertions.fail(
                              name
                                  + ": Expect replica "
                                  + fixedReplica
                                  + " not moved, but it appears to disappear from the target allocation");
                        });
              });
    }

    static void assertBrokerEmpty(ClusterInfo target, Predicate<Integer> clearBroker, String name) {
      var violated =
          target
              .replicaStream()
              .filter(i -> clearBroker.test(i.brokerId()))
              .collect(Collectors.toUnmodifiableSet());
      Assertions.assertTrue(
          violated.isEmpty(),
          name + ": the following replica should move to somewhere else " + violated);
    }
  }
}
