package org.astraea.balancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.admin.TopicPartition;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.ReplicaInfo;

public class ClusterLogAllocation {

  // guard by this
  private final Map<TopicPartition, List<LogPlacement>> allocation;

  public ClusterLogAllocation() {
    this.allocation = new HashMap<>();
  }

  private ClusterLogAllocation(Map<TopicPartition, List<LogPlacement>> allocation) {
    allocation.keySet().stream()
        .collect(Collectors.groupingBy(TopicPartition::topic))
        .forEach(
            (topic, tp) -> {
              int maxPartitionId =
                  tp.stream().mapToInt(TopicPartition::partition).max().orElseThrow();
              if ((maxPartitionId + 1) != tp.size())
                throw new IllegalArgumentException(
                    "The partition size of " + topic + " is illegal");
            });
    allocation.forEach(
        (tp, logs) -> {
          long uniqueBrokers = logs.stream().map(LogPlacement::broker).distinct().count();
          if (uniqueBrokers != logs.size() || logs.size() == 0)
            throw new IllegalArgumentException(
                "The topic "
                    + tp.topic()
                    + " partition "
                    + tp.partition()
                    + " has illegal replica set "
                    + logs);
        });
    this.allocation = allocation;
  }

  public static ClusterLogAllocation of(Map<TopicPartition, List<LogPlacement>> allocation) {
    final var map = new ConcurrentHashMap<TopicPartition, List<LogPlacement>>();
    allocation.forEach((tp, list) -> map.put(tp, new ArrayList<>(list)));
    return new ClusterLogAllocation(map);
  }

  public static ClusterLogAllocation of(ClusterInfo clusterInfo) {
    final Map<TopicPartition, List<LogPlacement>> allocation =
        clusterInfo.topics().stream()
            .map(clusterInfo::partitions)
            .flatMap(Collection::stream)
            .collect(
                Collectors.groupingBy(
                    replica ->
                        TopicPartition.of(replica.topic(), Integer.toString(replica.partition()))))
            .entrySet()
            .stream()
            .map(
                (entry) -> {
                  // validate if the given log placements are valid
                  if (entry.getValue().stream().filter(ReplicaInfo::isLeader).count() != 1)
                    throw new IllegalArgumentException(
                        "The " + entry.getKey() + " leader count mismatch 1.");

                  final var topicPartition = entry.getKey();
                  final var logPlacements =
                      entry.getValue().stream()
                          .sorted(Comparator.comparingInt(replica -> replica.isLeader() ? 0 : 1))
                          .map(
                              replica ->
                                  LogPlacement.of(
                                      replica.nodeInfo().id(), replica.dataFolder().orElse(null)))
                          .collect(Collectors.toList());

                  return Map.entry(topicPartition, logPlacements);
                })
            .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
    return new ClusterLogAllocation(allocation);
  }

  /** let specific broker leave the replica set and let another broker join the replica set. */
  public synchronized void migrateReplica(
      TopicPartition topicPartition, int broker, int destinationBroker) {
    final List<LogPlacement> sourceLogPlacements = this.allocation().get(topicPartition);
    if (sourceLogPlacements == null)
      throw new IllegalMigrationException(
          topicPartition.topic() + "-" + topicPartition.partition() + " no such topic/partition");
    int sourceLogIndex =
        IntStream.range(0, sourceLogPlacements.size())
            .filter(index -> sourceLogPlacements.get(index).broker() == broker)
            .findFirst()
            .orElse(-1);
    if (sourceLogIndex == -1)
      throw new IllegalMigrationException(
          broker + " is not part of the replica set for " + topicPartition);
    int destinationLogIndex =
        IntStream.range(0, sourceLogPlacements.size())
            .filter(index -> sourceLogPlacements.get(index).broker() == destinationBroker)
            .findFirst()
            .orElse(-1);
    if (destinationLogIndex != -1)
      throw new IllegalMigrationException(
          destinationBroker + " is already part of the replica set, no need to move");
    this.allocation.get(topicPartition).set(sourceLogIndex, LogPlacement.of(destinationBroker));
  }

  /** let specific follower log become the leader log of this topic/partition. */
  public synchronized void letReplicaBecomeLeader(
      TopicPartition topicPartition, int followerReplica) {
    final List<LogPlacement> sourceLogPlacements = this.allocation().get(topicPartition);
    if (sourceLogPlacements == null)
      throw new IllegalMigrationException(
          topicPartition.topic() + "-" + topicPartition.partition() + " no such topic/partition");
    int followerLogIndex =
        IntStream.range(0, sourceLogPlacements.size())
            .filter(index -> sourceLogPlacements.get(index).broker() == followerReplica)
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        followerReplica + " is not part of the replica set for " + topicPartition));
    int leaderLogIndex = 0;
    if (sourceLogPlacements.size() == 0) throw new IllegalStateException();
    if (leaderLogIndex == followerLogIndex) return; // nothing to do

    final var leaderLog = this.allocation.get(topicPartition).get(leaderLogIndex);
    final var followerLog = this.allocation.get(topicPartition).get(followerLogIndex);

    this.allocation.get(topicPartition).set(followerLogIndex, leaderLog);
    this.allocation.get(topicPartition).set(leaderLogIndex, followerLog);
  }

  /** change the data directory of specific log */
  public synchronized void changeDataDirectory(
      TopicPartition topicPartition, int broker, String path) {
    final List<LogPlacement> sourceLogPlacements = this.allocation().get(topicPartition);
    if (sourceLogPlacements == null)
      throw new IllegalMigrationException(
          topicPartition.topic() + "-" + topicPartition.partition() + " no such topic/partition");
    int sourceLogIndex =
        IntStream.range(0, sourceLogPlacements.size())
            .filter(index -> sourceLogPlacements.get(index).broker() == broker)
            .findFirst()
            .orElse(-1);
    if (sourceLogIndex == -1)
      throw new IllegalMigrationException(
          broker + " is not part of the replica set for " + topicPartition);
    final var oldLog = this.allocation.get(topicPartition).get(sourceLogIndex);
    final var newLog = LogPlacement.of(oldLog.broker(), path);
    this.allocation.get(topicPartition).set(sourceLogIndex, newLog);
  }

  /**
   * Access the allocation of the current log, noted that the return instance is an unmodifiable
   * map.
   */
  public Map<TopicPartition, List<LogPlacement>> allocation() {
    return Collections.unmodifiableMap(allocation);
  }

  // TODO: add a method to calculate the difference between two ClusterLogAllocation

}
