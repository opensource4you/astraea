package org.astraea.app.admin;

import java.util.List;
import java.util.Map;

/** used to migrate partitions to another broker or broker folder. */
public interface ReplicaMigrator {
  /**
   * move all partitions (leader replica and follower replicas) of topic
   *
   * @param topic topic name
   * @return this migrator
   */
  ReplicaMigrator topic(String topic);

  /**
   * move one partition (leader replica and follower replicas) of topic
   *
   * @param topic topic name
   * @param partition partition id
   * @return this migrator
   */
  ReplicaMigrator partition(String topic, int partition);

  /**
   * change the partition replica list. If the current partition leader is kicked out of the
   * partition replica list. A preferred leader election will occur implicitly. The preferred
   * leader(the first replica in the list) will become the new leader of this topic/partition. If
   * one wants the preferred leader election to occur explicitly. Consider using {@link
   * Admin#preferredLeaderElection(TopicPartition)}.
   *
   * @param brokers to host partitions
   */
  void moveTo(List<Integer> brokers);

  /**
   * move partition to specify folder.
   *
   * @param brokerFolders contain
   */
  void moveTo(Map<Integer, String> brokerFolders);
}
