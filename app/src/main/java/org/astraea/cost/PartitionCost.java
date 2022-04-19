package org.astraea.cost;

import java.util.Map;

/** Return type of cost function, `HasPartitionCost`. It returns the score of partitions. */
public interface PartitionCost {
  Map<TopicPartition, Double> value(String topic);

  Map<TopicPartition, Double> value(int brokerId);
}
