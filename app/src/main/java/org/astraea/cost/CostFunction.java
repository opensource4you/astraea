package org.astraea.cost;

import java.util.Map;
import org.astraea.metrics.collector.Fetcher;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public interface CostFunction {

  @Contract(value = " -> new", pure = true)
  static @NotNull CostFunction throughput() {
    return new DynamicWeightsLoadCost();
  }

  /**
   * score all nodes according to passed beans and cluster information.
   *
   * @param clusterInfo cluster information
   * @return the score of each node. the score range is [0 - 1]
   */
  Map<Integer, Double> cost(ClusterInfo clusterInfo);

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  Fetcher fetcher();
}
