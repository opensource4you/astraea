package org.astraea.cost;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface ClusterCost {
  double INVALID_COST = -1.0;

  /**
   * Create a ClusterCost by giving the broker's costs, and the set of all partition information.
   * */
  static ClusterCost scoreByBroker(
      List<PartitionInfo> partitionInfo, Map<Integer, Double> brokerCost) {
    return new ClusterCost() {
      @Override
      public Map<Integer, Double> brokerCost() {
        return Collections.unmodifiableMap(brokerCost);
      }

      /**
       * Assign the cost of each partition by the broker it locates.
       * */
      @Override
      public Map<TopicPartitionReplica, Double> partitionCost() {
        return partitionInfo.stream()
            .map(
                p ->
                    Map.entry(
                        TopicPartitionReplica.of(p.topic(), p.partition(), p.leader().id()),
                        brokerCost.getOrDefault(p.leader().id(), INVALID_COST)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    };
  }

  Map<Integer, Double> brokerCost();

  Map<TopicPartitionReplica, Double> partitionCost();
}
