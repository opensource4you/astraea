package org.astraea.partitioner.cost;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.partitioner.ClusterInfo;
import org.astraea.partitioner.NodeInfo;

public interface CostFunction {

  static CostFunction throughput() {
    return new ThroughputCost();
  }

  /**
   * score all nodes according to passed beans and cluster information.
   *
   * @param beans beans of each node
   * @param clusterInfo cluster information
   * @return the score of each node. the score range is [0 - 1]
   */
  Map<NodeInfo, Double> cost(Map<NodeInfo, List<HasBeanObject>> beans, ClusterInfo clusterInfo);

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  Collection<Function<MBeanClient, HasBeanObject>> metricsGetters();
}
