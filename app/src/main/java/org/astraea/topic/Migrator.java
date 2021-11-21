package org.astraea.topic;

import java.util.Map;
import java.util.Set;

/** used to migrate partitions to another broker or broker folder. */
public interface Migrator {
  /**
   * move all partitions (leader replica and follower replicas) of topic
   *
   * @param topic topic name
   * @return this migrator
   */
  Migrator topic(String topic);

  /**
   * move one partition (leader replica and follower replicas) of topic
   *
   * @param topic topic name
   * @param partition partition id
   * @return this migrator
   */
  Migrator partition(String topic, int partition);

  /**
   * move partitions to specify brokers. Noted that the number of brokers must be equal to the
   * number of replicas
   *
   * @param brokers to host partitions
   */
  void moveTo(Set<Integer> brokers);

  /**
   * move partition to specify folder.
   *
   * @param brokerFolders contain
   */
  void moveTo(Map<Integer, String> brokerFolders);
}
