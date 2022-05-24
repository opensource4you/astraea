package org.astraea.balancer.log;

import java.util.List;
import java.util.stream.Stream;
import org.astraea.admin.TopicPartition;

/**
 * Describe the log allocation state of a Kafka cluster. The implementation have to keep the cluster
 * log allocation information, provide method for query the placement, and offer a set of log
 * placement change operation.
 */
public interface ClusterLogAllocation {

  /** let specific broker leave the replica set and let another broker join the replica set. */
  void migrateReplica(TopicPartition topicPartition, int atBroker, int toBroker);

  /** let specific follower log become the leader log of this topic/partition. */
  void letReplicaBecomeLeader(TopicPartition topicPartition, int followerReplica);

  /** change the data directory of specific log */
  void changeDataDirectory(TopicPartition topicPartition, int atBroker, String newPath);

  /** Retrieve the log placements of specific {@link TopicPartition}. */
  List<LogPlacement> logPlacements(TopicPartition topicPartition);

  /** Retrieve the stream of all topic/partition pairs in allocation. */
  Stream<TopicPartition> topicPartitionStream();

  // TODO: add a method to calculate the difference between two ClusterLogAllocation
}
