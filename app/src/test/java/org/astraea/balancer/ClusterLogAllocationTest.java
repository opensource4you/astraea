package org.astraea.balancer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.admin.TopicPartition;
import org.astraea.balancer.log.ClusterLogAllocation;
import org.astraea.balancer.log.LogPlacement;
import org.astraea.cost.ClusterInfoProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ClusterLogAllocationTest {

  @Test
  void creation() {
    // empty replica set
    var badAllocation0 = Map.of(TopicPartition.of("topic", "0"), List.<LogPlacement>of());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ClusterLogAllocation.of(badAllocation0));

    // partial topic/partition
    var badAllocation1 = Map.of(TopicPartition.of("topic", "999"), List.of(LogPlacement.of(1001)));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ClusterLogAllocation.of(badAllocation1));

    // duplicate replica
    var badAllocation2 =
        Map.of(
            TopicPartition.of("topic", "0"),
            List.of(LogPlacement.of(1001), LogPlacement.of(1001), LogPlacement.of(1001)));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ClusterLogAllocation.of(badAllocation2));
  }

  @Test
  void of() {
    final ClusterLogAllocation immutable = ClusterLogAllocation.of(Map.of());
    final TopicPartition topicPartition = TopicPartition.of("topic", "0");
    final List<LogPlacement> logPlacements = List.of(LogPlacement.of(0));

    Assertions.assertThrows(
        Exception.class, () -> immutable.allocation().put(topicPartition, logPlacements));
  }

  @Test
  void migrateReplica() {
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(3, 1, 1, 1, (i) -> Set.of("topic"));
    final var clusterLogAllocation = ClusterLogAllocation.of(fakeClusterInfo);
    final var sourceTopicPartition = TopicPartition.of("topic", "0");

    clusterLogAllocation.migrateReplica(sourceTopicPartition, 0, 1);

    Assertions.assertEquals(
        1, clusterLogAllocation.allocation().get(sourceTopicPartition).get(0).broker());
    Assertions.assertDoesNotThrow(() -> ClusterLogAllocation.of(clusterLogAllocation.allocation()));
  }

  @Test
  void letReplicaBecomeLeader() {
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(3, 1, 1, 2, (i) -> Set.of("topic"));
    final var clusterLogAllocation = ClusterLogAllocation.of(fakeClusterInfo);
    final var sourceTopicPartition = TopicPartition.of("topic", "0");

    clusterLogAllocation.letReplicaBecomeLeader(sourceTopicPartition, 1);

    Assertions.assertEquals(
        1, clusterLogAllocation.allocation().get(sourceTopicPartition).get(0).broker());
    Assertions.assertEquals(
        0, clusterLogAllocation.allocation().get(sourceTopicPartition).get(1).broker());
    Assertions.assertDoesNotThrow(() -> ClusterLogAllocation.of(clusterLogAllocation.allocation()));
  }

  @Test
  void changeDataDirectory() {
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(3, 1, 1, 1, (i) -> Set.of("topic"));
    final var clusterLogAllocation = ClusterLogAllocation.of(fakeClusterInfo);
    final var sourceTopicPartition = TopicPartition.of("topic", "0");

    clusterLogAllocation.changeDataDirectory(sourceTopicPartition, 0, "/path/to/somewhere");

    Assertions.assertEquals(
        "/path/to/somewhere",
        clusterLogAllocation
            .allocation()
            .get(sourceTopicPartition)
            .get(0)
            .logDirectory()
            .orElseThrow());
    Assertions.assertDoesNotThrow(() -> ClusterLogAllocation.of(clusterLogAllocation.allocation()));
  }
}
