package org.astraea.partitioner.cost;

import java.util.List;
import java.util.Map;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.partitioner.ClusterInfo;

public interface CostFunction {

  static CostFunction throughput() {
    return new ThroughputCost();
  }

  /**
   * score all nodes according to passed beans and cluster information.
   *
   * @param beans beans of each node (key is node id)
   * @param clusterInfo cluster information
   * @return the score of each node. the score range is [0 - 1]
   */
  Map<Integer, Double> cost(Map<Integer, List<HasBeanObject>> beans, ClusterInfo clusterInfo);

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  Fetcher fetcher();
}
