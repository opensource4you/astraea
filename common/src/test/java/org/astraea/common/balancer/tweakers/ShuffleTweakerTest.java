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
package org.astraea.common.balancer.tweakers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoTest;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.FakeClusterInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ShuffleTweakerTest {

  @Test
  void testRun() {
    final var shuffleTweaker =
        ShuffleTweaker.builder()
            .numberOfShuffle(() -> ThreadLocalRandom.current().nextInt(1, 10))
            .build();
    final var fakeCluster = FakeClusterInfo.of(100, 10, 10, 3);
    final var stream = shuffleTweaker.generate(fakeCluster);
    final var iterator = stream.iterator();

    Assertions.assertDoesNotThrow(() -> System.out.println(iterator.next()));
    Assertions.assertDoesNotThrow(() -> System.out.println(iterator.next()));
    Assertions.assertDoesNotThrow(() -> System.out.println(iterator.next()));
  }

  @ParameterizedTest
  @ValueSource(ints = {3, 5, 7, 11, 13, 17, 19, 23, 29, 31})
  void testMovement(int shuffle) {
    final var fakeCluster = FakeClusterInfo.of(30, 30, 20, 5);
    final var shuffleTweaker = ShuffleTweaker.builder().numberOfShuffle(() -> shuffle).build();

    shuffleTweaker
        .generate(fakeCluster)
        .limit(100)
        .forEach(
            that -> {
              final var thisTps = fakeCluster.topicPartitions();
              final var thatTps = that.topicPartitions();
              final var thisMap =
                  thisTps.stream()
                      .collect(Collectors.toUnmodifiableMap(x -> x, fakeCluster::replicas));
              final var thatMap =
                  thatTps.stream().collect(Collectors.toUnmodifiableMap(x -> x, that::replicas));
              Assertions.assertEquals(thisTps, thatTps);
              Assertions.assertNotEquals(thisMap, thatMap);
            });
  }

  @Test
  void testNoNodes() {
    final var fakeCluster = FakeClusterInfo.of(0, 0, 0, 0);
    final var shuffleTweaker = ShuffleTweaker.builder().numberOfShuffle(() -> 3).build();

    Assertions.assertEquals(
        0, (int) shuffleTweaker.generate(fakeCluster).limit(100).count(), "No possible tweak");
  }

  @Test
  void testOneNode() {
    final var fakeCluster = FakeClusterInfo.of(1, 1, 1, 1, 1);
    final var shuffleTweaker = ShuffleTweaker.builder().numberOfShuffle(() -> 3).build();

    Assertions.assertEquals(
        0, (int) shuffleTweaker.generate(fakeCluster).limit(100).count(), "No possible tweak");
  }

  @Test
  void testNoTopic() {
    final var fakeCluster = FakeClusterInfo.of(3, 0, 0, 0);
    final var shuffleTweaker = ShuffleTweaker.builder().numberOfShuffle(() -> 3).build();

    Assertions.assertEquals(
        0, (int) shuffleTweaker.generate(fakeCluster).limit(100).count(), "No possible tweak");
  }

  @Test
  void parallelStreamWorks() {
    final var shuffleTweaker =
        ShuffleTweaker.builder()
            .numberOfShuffle(() -> ThreadLocalRandom.current().nextInt(1, 10))
            .build();
    final var fakeCluster = FakeClusterInfo.of(10, 20, 10, 3);

    // generator can do parallel without error.
    Assertions.assertDoesNotThrow(
        () -> shuffleTweaker.generate(fakeCluster).parallel().limit(100).count());
  }

  @Test
  @Disabled
  void parallelPerformanceTests() throws InterruptedException {
    final var shuffleTweaker =
        ShuffleTweaker.builder()
            .numberOfShuffle(() -> ThreadLocalRandom.current().nextInt(1, 10))
            .build();
    final var fakeCluster = FakeClusterInfo.of(50, 500, 30, 2);
    final var counter = new LongAdder();
    final var forkJoinPool = new ForkJoinPool(ForkJoinPool.getCommonPoolParallelism());
    final var startTime = System.nanoTime();

    forkJoinPool.submit(
        () ->
            shuffleTweaker
                .generate(fakeCluster)
                .parallel()
                .forEach((ignore) -> counter.increment()));

    // report progress
    final var reportThread =
        new Thread(
            () -> {
              while (!Thread.currentThread().isInterrupted()) {
                Utils.sleep(Duration.ofSeconds(1));
                long now = System.nanoTime();
                System.out.printf("%.3f proposal/s %n", counter.sum() / ((now - startTime) / 1e9));
              }
            });

    reportThread.start();
    reportThread.join();
    forkJoinPool.shutdownNow();
  }

  @Test
  void testEligiblePartition() {
    final var shuffleTweaker = ShuffleTweaker.builder().numberOfShuffle(() -> 100).build();
    var dataDir =
        Map.of(
            0, Set.of("/a", "/b", "c"),
            1, Set.of("/a", "/b", "c"),
            2, Set.of("/a", "/b", "c"));
    var nodeA = NodeInfo.of(0, "", -1);
    var nodeB = NodeInfo.of(1, "", -1);
    var nodeC = NodeInfo.of(2, "", -1);
    var base =
        Replica.builder()
            .topic("topic")
            .partition(0)
            .broker(nodeA)
            .lag(0)
            .size(0)
            .isLeader(false)
            .isSync(true)
            .isFuture(false)
            .isOffline(false)
            .isPreferredLeader(false)
            .path("/a")
            .build();
    var allocation =
        ClusterInfoTest.of(
            List.of(
                Replica.builder(base)
                    .topic("normal-topic")
                    .isLeader(true)
                    .isPreferredLeader(true)
                    .build(),
                Replica.builder(base).topic("normal-topic").broker(nodeB).build(),
                Replica.builder(base).topic("normal-topic").broker(nodeC).build(),
                Replica.builder(base)
                    .topic("offline-single")
                    .isPreferredLeader(true)
                    .isOffline(true)
                    .build(),
                Replica.builder(base)
                    .topic("no-leader")
                    .isPreferredLeader(true)
                    .broker(nodeA)
                    .build(),
                Replica.builder(base).topic("no-leader").broker(nodeB).build(),
                Replica.builder(base).topic("no-leader").broker(nodeC).build()));
    shuffleTweaker
        .generate(allocation)
        .limit(30)
        .forEach(
            newAllocation -> {
              var notFulfilled = ClusterInfo.findNonFulfilledAllocation(allocation, newAllocation);
              Assertions.assertTrue(
                  notFulfilled.stream()
                      .map(TopicPartition::topic)
                      .allMatch(x -> x.equals("normal-topic")),
                  "only normal-topic get altered. Actual: " + notFulfilled);
            });
  }
}
