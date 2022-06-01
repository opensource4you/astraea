package org.astraea.admin;

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
   * move partitions to specify brokers. Noted that this method won't invoke leader election
   * explicitly.
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
