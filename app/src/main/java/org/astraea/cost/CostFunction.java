package org.astraea.cost;

import java.util.Map;
import org.astraea.metrics.collector.Fetcher;

public interface CostFunction {

  static CostFunction throughput() {
    return new ThroughputCost();
  }

  /**
   * score all nodes according to passed beans and cluster information.
   *
   * @param clusterInfo cluster information
   * @return the score of each partition/replica. the score range is [0 - 1]
   */
  Map<TopicPartitionReplica, Double> cost(ClusterInfo clusterInfo);

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  Fetcher fetcher();
}
