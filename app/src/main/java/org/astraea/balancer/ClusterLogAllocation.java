package org.astraea.balancer;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.astraea.admin.TopicPartition;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.ReplicaInfo;

public class ClusterLogAllocation {

  // guard by this
  private final Map<TopicPartition, List<LogPlacement>> allocation;

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
    return new ClusterLogAllocation(new ConcurrentHashMap<>(allocation));
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
                          .collect(Collectors.toUnmodifiableList());

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
    if (sourceLogPlacements.stream().noneMatch(log -> log.broker() == broker))
      throw new IllegalMigrationException(
          broker + " is not part of the replica set for " + topicPartition);
    if (sourceLogPlacements.stream().anyMatch(log -> log.broker() == destinationBroker))
      throw new IllegalMigrationException(
          destinationBroker + " is already part of the replica set, no need to move");
    final List<LogPlacement> finalLogPlacements =
        sourceLogPlacements.stream()
            .map(log -> log.broker() == broker ? LogPlacement.of(destinationBroker) : log)
            .collect(Collectors.toUnmodifiableList());

    this.allocation.put(topicPartition, finalLogPlacements);
  }

  /** let specific follower log become the leader log of this topic/partition. */
  public synchronized void letReplicaBecomeLeader(
      TopicPartition topicPartition, int followerReplica) {
    final List<LogPlacement> sourceLogPlacements = this.allocation().get(topicPartition);
    if (sourceLogPlacements == null)
      throw new IllegalMigrationException(
          topicPartition.topic() + "-" + topicPartition.partition() + " no such topic/partition");
    final LogPlacement followerLog =
        sourceLogPlacements.stream()
            .filter(log -> log.broker() == followerReplica)
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        followerReplica + " is not part of the replica set for " + topicPartition));
    final LogPlacement leaderLog = sourceLogPlacements.stream().findFirst().orElseThrow();
    if (leaderLog.broker() == followerLog.broker()) return; // nothing to do
    final List<LogPlacement> finalLogPlacements =
        sourceLogPlacements.stream()
            .map(
                log -> {
                  if (log.broker() == followerLog.broker()) return leaderLog;
                  else if (log.broker() == leaderLog.broker()) return followerLog;
                  else return log;
                })
            .collect(Collectors.toUnmodifiableList());

    this.allocation.put(topicPartition, finalLogPlacements);
  }

  /** change the data directory of specific log */
  public synchronized void changeDataDirectory(
      TopicPartition topicPartition, int broker, String path) {
    final List<LogPlacement> sourceLogPlacements = this.allocation().get(topicPartition);
    if (sourceLogPlacements == null)
      throw new IllegalMigrationException(
          topicPartition.topic() + "-" + topicPartition.partition() + " no such topic/partition");
    if (sourceLogPlacements.stream().noneMatch(log -> log.broker() == broker))
      throw new IllegalMigrationException(
          broker + " is not part of the replica set for " + topicPartition);
    final List<LogPlacement> finalLogPlacements =
        sourceLogPlacements.stream()
            .map(log -> log.broker() == broker ? LogPlacement.of(broker, path) : log)
            .collect(Collectors.toUnmodifiableList());

    this.allocation.put(topicPartition, finalLogPlacements);
  }

  /**
   * Access the allocation of the current log, noted that the return instance is an immutable copy.
   */
  public Map<TopicPartition, List<LogPlacement>> allocation() {
    return Map.copyOf(allocation);
  }

  // TODO: add a method to calculate the difference between two ClusterLogAllocation

}
