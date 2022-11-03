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
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.FakeClusterInfo;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ShuffleTweakerTest {

  @Test
  void testRun() {
    final var shuffleTweaker = new ShuffleTweaker(5, 10);
    final var fakeCluster = FakeClusterInfo.of(100, 10, 10, 3);
    final var stream =
        shuffleTweaker.generate(
            fakeCluster.dataDirectories(), ClusterLogAllocation.of(fakeCluster));
    final var iterator = stream.iterator();

    Assertions.assertDoesNotThrow(() -> System.out.println(iterator.next()));
    Assertions.assertDoesNotThrow(() -> System.out.println(iterator.next()));
    Assertions.assertDoesNotThrow(() -> System.out.println(iterator.next()));
  }

  @ParameterizedTest
  @ValueSource(ints = {3, 5, 7, 11, 13, 17, 19, 23, 29, 31})
  void testMovement(int shuffle) {
    final var fakeCluster = FakeClusterInfo.of(30, 30, 20, 5);
    final var allocation = ClusterLogAllocation.of(fakeCluster);
    final var shuffleTweaker = new ShuffleTweaker(() -> shuffle);

    shuffleTweaker
        .generate(fakeCluster.dataDirectories(), ClusterLogAllocation.of(fakeCluster))
        .limit(100)
        .forEach(
            that -> {
              final var thisTps = allocation.topicPartitions();
              final var thatTps = that.topicPartitions();
              final var thisMap =
                  thisTps.stream()
                      .collect(Collectors.toUnmodifiableMap(x -> x, allocation::replicas));
              final var thatMap =
                  thatTps.stream().collect(Collectors.toUnmodifiableMap(x -> x, that::replicas));
              Assertions.assertEquals(thisTps, thatTps);
              Assertions.assertNotEquals(thisMap, thatMap);
            });
  }

  @Test
  void testNoNodes() {
    final var fakeCluster = FakeClusterInfo.of(0, 0, 0, 0);
    final var shuffleTweaker = new ShuffleTweaker(() -> 3);

    Assertions.assertEquals(
        0,
        (int)
            shuffleTweaker
                .generate(fakeCluster.dataDirectories(), ClusterLogAllocation.of(fakeCluster))
                .count(),
        "No possible tweak");
  }

  @Test
  void testOneNode() {
    final var fakeCluster = FakeClusterInfo.of(1, 1, 1, 1);
    final var shuffleTweaker = new ShuffleTweaker(() -> 3);

    Assertions.assertEquals(
        0,
        (int)
            shuffleTweaker
                .generate(
                    fakeCluster.dataDirectories().entrySet().stream()
                        .limit(1)
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().stream().limit(1).collect(Collectors.toSet()))),
                    ClusterLogAllocation.of(fakeCluster))
                .count(),
        "No possible tweak");
  }

  @Test
  void testNoTopic() {
    final var fakeCluster = FakeClusterInfo.of(3, 0, 0, 0);
    final var shuffleTweaker = new ShuffleTweaker(() -> 3);

    Assertions.assertEquals(
        0,
        (int)
            shuffleTweaker
                .generate(fakeCluster.dataDirectories(), ClusterLogAllocation.of(fakeCluster))
                .count(),
        "No possible tweak");
  }

  @Disabled
  @ParameterizedTest(name = "[{0}] {1} nodes, {2} topics, {3} partitions, {4} replicas")
  @CsvSource(
      value = {
        //      scenario, node, topic, partition, replica
        "  small cluster,    3,    10,        30,       3",
        " medium cluster,   30,    50,        50,       3",
        "    big cluster,  100,   100,       100,       1",
        "  many replicas, 1000,    30,       100,      30",
      })
  void performanceTest(
      String scenario, int nodeCount, int topicCount, int partitionCount, int replicaCount) {
    // This test is not intended for any performance guarantee.
    // It only served the purpose of keeping track of the generator performance change in the CI
    // log.
    // Notice: Stream#limit() will hurt performance. the number here might not reflect the actual
    // performance.
    final var shuffleTweaker = new ShuffleTweaker(0, 10);
    final var fakeCluster = FakeClusterInfo.of(nodeCount, topicCount, partitionCount, replicaCount);
    final var size = 1000;

    final long s = System.nanoTime();
    final var count =
        shuffleTweaker
            .generate(fakeCluster.dataDirectories(), ClusterLogAllocation.of(fakeCluster))
            .limit(size)
            .count();
    final long t = System.nanoTime();
    Assertions.assertEquals(size, count);
    System.out.printf("[%s]%n", scenario);
    System.out.printf(
        "%d nodes, %d topics, %d partitions, %d replicas.%n",
        nodeCount, topicCount, partitionCount, replicaCount);
    System.out.printf("Generate %.3f proposals per second.%n", count / (((double) (t - s) / 1e9)));
    System.out.println();
  }

  @Test
  void parallelStreamWorks() {
    final var shuffleTweaker = new ShuffleTweaker(0, 10);
    final var fakeCluster = FakeClusterInfo.of(10, 20, 10, 3);

    // generator can do parallel without error.
    Assertions.assertDoesNotThrow(
        () ->
            shuffleTweaker
                .generate(fakeCluster.dataDirectories(), ClusterLogAllocation.of(fakeCluster))
                .parallel()
                .limit(100)
                .count());
  }

  @Test
  @Disabled
  void parallelPerformanceTests() throws InterruptedException {
    final var shuffleTweaker = new ShuffleTweaker(0, 10);
    final var fakeCluster = FakeClusterInfo.of(50, 500, 30, 2);
    final var counter = new LongAdder();
    final var forkJoinPool = new ForkJoinPool(ForkJoinPool.getCommonPoolParallelism());
    final var startTime = System.nanoTime();

    forkJoinPool.submit(
        () ->
            shuffleTweaker
                .generate(fakeCluster.dataDirectories(), ClusterLogAllocation.of(fakeCluster))
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
    var shuffleTweaker = new ShuffleTweaker(() -> 100);
    var dataDir =
        Map.of(
            0, Set.of("/a", "/b", "c"),
            1, Set.of("/a", "/b", "c"),
            2, Set.of("/a", "/b", "c"));
    var nodeA = NodeInfo.of(0, "", -1);
    var nodeB = NodeInfo.of(1, "", -1);
    var nodeC = NodeInfo.of(2, "", -1);
    var base = Replica.of("topic", 0, nodeA, 0, 0, false, true, false, false, false, "/a");
    var allocation =
        ClusterLogAllocation.of(
            ClusterInfo.of(
                List.of(
                    Replica.builder(base)
                        .topic("normal-topic")
                        .isLeader(true)
                        .isPreferredLeader(true)
                        .build(),
                    Replica.builder(base).topic("normal-topic").nodeInfo(nodeB).build(),
                    Replica.builder(base).topic("normal-topic").nodeInfo(nodeC).build(),
                    Replica.builder(base)
                        .topic("offline-single")
                        .isPreferredLeader(true)
                        .isOffline(true)
                        .build(),
                    Replica.builder(base)
                        .topic("no-leader")
                        .isPreferredLeader(true)
                        .nodeInfo(nodeA)
                        .build(),
                    Replica.builder(base).topic("no-leader").nodeInfo(nodeB).build(),
                    Replica.builder(base).topic("no-leader").nodeInfo(nodeC).build())));
    shuffleTweaker
        .generate(dataDir, allocation)
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
