package org.astraea.balancer.alpha.cost;

import java.util.Map;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.cost.ClusterInfo;
import org.astraea.metrics.collector.Fetcher;

public interface CostFunction {
  /**
   * score all replicas according to passed beans and cluster information.
   *
   * @param clusterInfo cluster information
   * @return the score of each node. the score range is [0 - 1]
   */
  Map<TopicPartitionReplica, Double> cost(ClusterInfo clusterInfo) throws InterruptedException;

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  Fetcher fetcher();
}
