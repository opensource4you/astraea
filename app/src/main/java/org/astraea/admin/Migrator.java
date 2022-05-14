package org.astraea.admin;

import java.util.List;
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
   * move partitions to specify brokers. Noted that this method won't invoke leader election
   * explicitly.
   *
   * @param brokers to host partitions
   */
  void moveTo(List<Integer> brokers);

  /**
   * move the leader and followers to specify nodes. Noted that this method will invoke leader
   * election.
   *
   * @param leader the node to host leader
   * @param followers the nodes to host followers
   */
  void moveTo(int leader, Set<Integer> followers);

  /**
   * move partition to specify folder.
   *
   * @param brokerFolders contain
   */
  void moveTo(Map<Integer, String> brokerFolders);
}
