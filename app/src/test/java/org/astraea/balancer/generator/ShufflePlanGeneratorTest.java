package org.astraea.balancer.generator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.astraea.admin.TopicPartition;
import org.astraea.balancer.ClusterLogAllocation;
import org.astraea.balancer.LogPlacement;
import org.astraea.cost.ClusterInfoProvider;
import org.junit.jupiter.api.Assertions;
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
  @ValueSource(ints = {3, 5, 7, 11, 13, 17, 19, 23, 29, 31})
  void testMovement(int shuffle) {
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(10, 10, 10, 3);
    final var innerLogAllocation = ClusterLogAllocation.of(fakeClusterInfo);
    final var shufflePlanGenerator = new ShufflePlanGenerator(() -> shuffle);
    final var counter = new LongAdder();

    final var mockedAllocation =
        new ClusterLogAllocation() {

          @Override
          public synchronized void migrateReplica(
              TopicPartition topicPartition, int broker, int destinationBroker) {
            counter.increment();
            innerLogAllocation.migrateReplica(topicPartition, broker, destinationBroker);
          }

          @Override
          public synchronized void letReplicaBecomeLeader(
              TopicPartition topicPartition, int followerReplica) {
            counter.increment();
            innerLogAllocation.letReplicaBecomeLeader(topicPartition, followerReplica);
          }

          @Override
          public synchronized void changeDataDirectory(
              TopicPartition topicPartition, int broker, String path) {
            counter.increment();
            innerLogAllocation.changeDataDirectory(topicPartition, broker, path);
          }

          @Override
          public Map<TopicPartition, List<LogPlacement>> allocation() {
            return innerLogAllocation.allocation();
          }
        };

    final var iterator =
        shufflePlanGenerator.generate(fakeClusterInfo, mockedAllocation).iterator();

    for (int i = 0; i < 100; i++) {
      iterator.next();
      Assertions.assertEquals(shuffle, counter.sum());
      counter.reset();
    }
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

  @ParameterizedTest(
      name = "[{0}] {1} nodes, {2} topics, {3} partitions, {4} replicas (parallel: {5})")
  @CsvSource(
      value = {
        //      scenario, node, topic, partition, replica,   parallel
        "    small cluster,    3,    10,        30,       3, false   ",
        "    small cluster,    3,    10,        30,       3, true    ",
        "   medium cluster,   30,    50,        50,       3, false   ",
        "   medium cluster,   30,    50,        50,       3, true    ",
        "      big cluster,  100,   100,       100,       1, false   ",
        "      big cluster,  100,   100,       100,       1, true    ",
        "    many replicas, 1000,    30,       100,      30, false   ",
        "    many replicas, 1000,    30,       100,      30, true    ",
      })
  void performanceTest(
      String scenario,
      int nodeCount,
      int topicCount,
      int partitionCount,
      int replicaCount,
      boolean parallel) {
    // This test is not intended for any performance guarantee.
    // It only served the purpose of keeping track of the generator performance change in the CI
    // log.
    final var shufflePlanGenerator = new ShufflePlanGenerator(0, 10);
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(nodeCount, topicCount, partitionCount, replicaCount);
    final var size = 1000;

    final long s = System.nanoTime();
    final var count =
        parallel
            ? shufflePlanGenerator.generate(fakeClusterInfo).parallel().limit(size).count()
            : shufflePlanGenerator.generate(fakeClusterInfo).limit(size).count();
    final long t = System.nanoTime();
    Assertions.assertEquals(size, count);
    System.out.printf("[%s] (%s)%n", scenario, parallel ? "Parallel" : "Sequential");
    System.out.printf(
        "%d nodes, %d topics, %d partitions, %d replicas.%n",
        nodeCount, topicCount, partitionCount, replicaCount);
    System.out.printf("Generate %.3f proposals per second.%n", count / (((double) (t - s) / 1e9)));
    System.out.println();
  }
}
