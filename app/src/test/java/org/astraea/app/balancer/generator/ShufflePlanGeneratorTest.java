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
package org.astraea.app.balancer.generator;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;
import org.astraea.app.cost.ClusterInfoProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ShufflePlanGeneratorTest {

  @Test
  void testRun() {
    final var shufflePlanGenerator = new ShufflePlanGenerator(5, 10);
    final var fakeCluster = ClusterInfoProvider.fakeClusterInfo(100, 10, 10, 3);
    final var stream = shufflePlanGenerator.generate(fakeCluster);
    final var iterator = stream.iterator();

    Assertions.assertDoesNotThrow(() -> System.out.println(iterator.next()));
    Assertions.assertDoesNotThrow(() -> System.out.println(iterator.next()));
    Assertions.assertDoesNotThrow(() -> System.out.println(iterator.next()));
  }

  @ParameterizedTest
  @ValueSource(ints = {3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 301})
  void testMovement(int shuffle) {
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(30, 30, 20, 5);
    final var allocation = LayeredClusterLogAllocation.of(fakeClusterInfo);
    final var shufflePlanGenerator = new ShufflePlanGenerator(() -> shuffle);

    shufflePlanGenerator
        .generate(fakeClusterInfo)
        .limit(100)
        .forEach(
            proposal -> {
              final var that = proposal.rebalancePlan().orElseThrow();
              final var thisTps =
                  allocation.topicPartitionStream().collect(Collectors.toUnmodifiableSet());
              final var thatTps =
                  that.topicPartitionStream().collect(Collectors.toUnmodifiableSet());
              final var thisMap =
                  thisTps.stream()
                      .collect(Collectors.toUnmodifiableMap(x -> x, allocation::logPlacements));
              final var thatMap =
                  thatTps.stream()
                      .collect(Collectors.toUnmodifiableMap(x -> x, that::logPlacements));
              Assertions.assertEquals(thisTps, thatTps);
              Assertions.assertNotEquals(thisMap, thatMap);
            });
  }

  @Test
  void testNoNodes() {
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(0, 0, 0, 0);
    final var shufflePlanGenerator = new ShufflePlanGenerator(() -> 3);

    final var proposal = shufflePlanGenerator.generate(fakeClusterInfo).iterator().next();

    System.out.println(proposal);
    Assertions.assertFalse(proposal.rebalancePlan().isPresent());
    Assertions.assertTrue(proposal.warnings().size() >= 1);
  }

  @Test
  void testOneNode() {
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(1, 1, 1, 1);
    final var shufflePlanGenerator = new ShufflePlanGenerator(() -> 3);

    final var proposal = shufflePlanGenerator.generate(fakeClusterInfo).iterator().next();

    System.out.println(proposal);
    Assertions.assertFalse(proposal.rebalancePlan().isPresent());
    Assertions.assertTrue(proposal.warnings().size() >= 1);
  }

  @Test
  void testNoTopic() {
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(3, 0, 0, 0);
    final var shufflePlanGenerator = new ShufflePlanGenerator(() -> 3);

    final var proposal = shufflePlanGenerator.generate(fakeClusterInfo).iterator().next();

    System.out.println(proposal);
    Assertions.assertFalse(proposal.rebalancePlan().isPresent());
    Assertions.assertTrue(proposal.warnings().size() >= 1);
  }

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
    final var shufflePlanGenerator = new ShufflePlanGenerator(0, 10);
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(nodeCount, topicCount, partitionCount, replicaCount);
    final var size = 1000;

    final long s = System.nanoTime();
    final var count = shufflePlanGenerator.generate(fakeClusterInfo).limit(size).count();
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
    final var shufflePlanGenerator = new ShufflePlanGenerator(0, 10);
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(10, 20, 10, 3);

    // generator can do parallel without error.
    Assertions.assertDoesNotThrow(
        () -> shufflePlanGenerator.generate(fakeClusterInfo).parallel().limit(100).count());
  }

  @Test
  @Disabled
  void parallelPerformanceTests() throws InterruptedException {
    final var shufflePlanGenerator = new ShufflePlanGenerator(0, 10);
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(50, 500, 30, 2);
    final var counter = new LongAdder();
    final var forkJoinPool = new ForkJoinPool(ForkJoinPool.getCommonPoolParallelism());
    final var startTime = System.nanoTime();

    forkJoinPool.submit(
        () ->
            shufflePlanGenerator
                .generate(fakeClusterInfo)
                .parallel()
                .forEach((ignore) -> counter.increment()));

    // report progress
    final var reportThread =
        new Thread(
            () -> {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  TimeUnit.SECONDS.sleep(1);
                  long now = System.nanoTime();
                  System.out.printf(
                      "%.3f proposal/s %n", counter.sum() / ((now - startTime) / 1e9));
                } catch (InterruptedException e) {
                  break;
                }
              }
            });

    reportThread.start();
    reportThread.join();
    forkJoinPool.shutdownNow();
  }
}
