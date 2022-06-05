package org.astraea.app.cost;

import java.util.Map;
import org.astraea.app.admin.TopicPartition;

/** Return type of cost function, `HasPartitionCost`. It returns the score of partitions. */
public interface PartitionCost {
  /**
   * Get the cost of all leader partitions with the given topic name.
   *
   * @param topic the topic name we want to query for.
   * @return the cost of all leader partitions, with respect to the given topic.
   */
  Map<TopicPartition, Double> value(String topic);

  /**
   * Get the cost of all partitions (leader/followers) with the given broker ID.
   *
   * @param brokerId the broker we want to query for.
   * @return the cost of all partitions (leader/followers), with respect to the given broker.
   */
  Map<TopicPartition, Double> value(int brokerId);
}
