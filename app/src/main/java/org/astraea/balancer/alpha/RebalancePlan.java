package org.astraea.balancer.alpha;

import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

public class RebalancePlan {

  /** The topic/partition/replica allocation before rebalance plan execution */
  private final Map<String, Map<Integer, Set<Integer>>> allocationBeforeRebalance;

  /** The topic/partition/replica allocation after rebalance plan executed */
  private final Map<String, Map<Integer, Set<Integer>>> allocationAfterRebalance;

  /** The preferred leader before rebalance plan execution */
  private final Map<TopicPartition, Integer> preferredLeaderBeforeRebalance;

  /** The preferred leader after rebalance plan executed */
  private final Map<TopicPartition, Integer> preferredLeaderAfterRebalance;

  public RebalancePlan(
      Map<String, Map<Integer, Set<Integer>>> allocationBeforeRebalance,
      Map<String, Map<Integer, Set<Integer>>> allocationAfterRebalance,
      Map<TopicPartition, Integer> preferredLeaderBeforeRebalance,
      Map<TopicPartition, Integer> preferredLeaderAfterRebalance) {
    this.allocationBeforeRebalance = Map.copyOf(allocationBeforeRebalance);
    this.allocationAfterRebalance = Map.copyOf(allocationAfterRebalance);
    this.preferredLeaderBeforeRebalance = Map.copyOf(preferredLeaderBeforeRebalance);
    this.preferredLeaderAfterRebalance = Map.copyOf(preferredLeaderAfterRebalance);
  }

  public Map<String, Map<Integer, Set<Integer>>> allocationBeforeRebalance() {
    return allocationBeforeRebalance;
  }

  public Map<String, Map<Integer, Set<Integer>>> allocationAfterRebalance() {
    return allocationAfterRebalance;
  }

  public Map<TopicPartition, Integer> preferredLeaderBeforeRebalance() {
    return preferredLeaderBeforeRebalance;
  }

  public Map<TopicPartition, Integer> preferredLeaderAfterRebalance() {
    return preferredLeaderAfterRebalance;
  }
}
