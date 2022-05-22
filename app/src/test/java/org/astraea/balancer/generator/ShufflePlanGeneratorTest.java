package org.astraea.balancer.generator;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
import org.astraea.admin.TopicPartition;
import org.astraea.balancer.ClusterLogAllocation;
import org.astraea.balancer.LogPlacement;
import org.astraea.balancer.RebalancePlanProposal;
import org.astraea.cost.ClusterInfoProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

  @RepeatedTest(10)
  void testMovement() {
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(
            100, 3, 10, 1, (ignore) -> Set.of("breaking-news", "chess", "animal"));
    final var shuffleCount = 1;
    final var shuffleSourceTopicPartition = TopicPartition.of("breaking-news", "0");
    final var shuffleSourceLogs =
        ClusterLogAllocation.of(fakeClusterInfo).allocation().get(shuffleSourceTopicPartition);
    final var shufflePlanGenerator =
        new ShufflePlanGenerator(() -> shuffleCount) {
          @Override
          int sourceTopicPartitionSelector(List<TopicPartition> migrationCandidates) {
            return IntStream.range(0, migrationCandidates.size())
                .filter(i -> migrationCandidates.get(i).equals(shuffleSourceTopicPartition))
                .findFirst()
                .orElseThrow();
          }

          @Override
          int sourceLogPlacementSelector(List<LogPlacement> migrationCandidates) {
            return super.sourceLogPlacementSelector(migrationCandidates);
          }

          @Override
          int migrationSelector(List<ShufflePlanGenerator.Movement> movementCandidates) {
            return super.migrationSelector(movementCandidates);
          }
        };

    RebalancePlanProposal proposal =
        shufflePlanGenerator.generate(fakeClusterInfo).iterator().next();
    System.out.println(proposal);

    Assertions.assertNotEquals(
        shuffleSourceLogs,
        proposal.rebalancePlan().orElseThrow().allocation().get(shuffleSourceTopicPartition));
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
  @CsvSource(value = {
          //      scenario, node, topic, partition, replica
          "  small cluster,    3,    10,        30,       3",
          " medium cluster,   30,    50,        50,       3",
          "    big cluster,  100,   100,       100,       1",
          "  many replicas, 1000,    30,       100,      30",
  })
  void performanceTest(String scenario, int nodeCount, int topicCount, int partitionCount, int replicaCount) {
    // This test is not intended for any performance guarantee.
    // It only served the purpose of keeping track of the generator performance change in the CI log.
    final var shufflePlanGenerator = new ShufflePlanGenerator(0, 10);
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(nodeCount, topicCount, partitionCount, replicaCount);
    final var size = 1000;

    final long s = System.nanoTime();
    final var count = shufflePlanGenerator.generate(fakeClusterInfo)
            .limit(size)
            .count();
    final long t = System.nanoTime();
    Assertions.assertEquals(size, count);
    System.out.printf("[%s]%n", scenario);
    System.out.printf("%d nodes, %d topics, %d partitions, %d replicas.%n",
            nodeCount,
            topicCount,
            partitionCount,
            replicaCount);
    System.out.printf("Generate %.3f proposals per second.%n", count / (((double)(t - s)/ 1e9)));
    System.out.println();
  }
}
