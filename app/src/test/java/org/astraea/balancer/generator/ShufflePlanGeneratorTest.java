package org.astraea.balancer.generator;

import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.astraea.balancer.ClusterLogAllocation;
import org.astraea.balancer.LogPlacement;
import org.astraea.balancer.RebalancePlanProposal;
import org.astraea.cost.ClusterInfoProvider;
import org.astraea.cost.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

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
    final var shuffleSourceTopicPartition = TopicPartition.of("breaking-news", 0);
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
    Assertions.assertFalse(proposal.isPlanGenerated());
    Assertions.assertTrue(proposal.warnings().size() >= 1);
  }

  @Test
  void testOneNode() {
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(1, 1, 1, 1);
    final var shufflePlanGenerator = new ShufflePlanGenerator(() -> 3);

    final var proposal = shufflePlanGenerator.generate(fakeClusterInfo).iterator().next();

    System.out.println(proposal);
    Assertions.assertFalse(proposal.isPlanGenerated());
    Assertions.assertTrue(proposal.warnings().size() >= 1);
  }

  @Test
  void testNoTopic() {
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(3, 0, 0, 0);
    final var shufflePlanGenerator = new ShufflePlanGenerator(() -> 3);

    final var proposal = shufflePlanGenerator.generate(fakeClusterInfo).iterator().next();

    System.out.println(proposal);
    Assertions.assertFalse(proposal.isPlanGenerated());
    Assertions.assertTrue(proposal.warnings().size() >= 1);
  }
}
