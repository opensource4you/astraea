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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoBuilder;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.junit.jupiter.api.Assertions;

/** A collection of helper methods that aid in verifying the implementation of balancer configs. */
public class BalancerConfigTestSuite {

  public static void testBalancerAllowedTopicRegex(Balancer balancer) {
    final var cluster = cluster(10, 10, 10, (short) 5);
    final var AssertionsHelper =
        new Object() {
          void assertSomeMovement(ClusterInfo source, ClusterInfo target, String name) {
            Assertions.assertNotEquals(
                Set.of(),
                ClusterInfo.findNonFulfilledAllocation(source, target),
                name + ": Should have movements");
          }

          void assertOnlyAllowedMovement(
              ClusterInfo source, ClusterInfo target, Pattern allowed, String name) {
            assertSomeMovement(source, target, name);
            Assertions.assertEquals(
                Set.of(),
                ClusterInfo.findNonFulfilledAllocation(source, target).stream()
                    .filter(Predicate.not((tp) -> allowed.asMatchPredicate().test(tp.topic())))
                    .collect(Collectors.toUnmodifiableSet()),
                name + ": Only allowed topics been altered.");
          }

          void assertNoMovement(ClusterInfo source, ClusterInfo target, String name) {
            Assertions.assertEquals(
                Set.of(),
                ClusterInfo.findNonFulfilledAllocation(source, target),
                name + ": Should have no movement");
          }
        };

    {
      var testName = "[test no limit]";
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  // This argument is not applied
                  // .config(BalancerCapabilities.BALANCER_ALLOWED_TOPIC_REGEX, regexRaw)
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
                  .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, regexRaw)
                  .build());
      AssertionsHelper.assertOnlyAllowedMovement(
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
                  .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, regexRaw)
                  .build());
      AssertionsHelper.assertNoMovement(cluster, plan.orElseThrow().proposal(), testName);
    }
  }

  private static ClusterInfo cluster(int nodes, int topics, int partitions, short replicas) {
    var builder =
        ClusterInfoBuilder.builder()
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

  public static HasClusterCost decreasingCost() {
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
}
