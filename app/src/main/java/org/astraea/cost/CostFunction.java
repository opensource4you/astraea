package org.astraea.cost;

import org.astraea.metrics.collector.Fetcher;

/**
 * It is meaningless to implement this interface. Instead, we should implement interfaces like
 * {@link HasBrokerCost} or {@link HasPartitionCost}.
 */
public interface CostFunction {

  static CostFunction throughput() {
    return new ThroughputCost();
  }

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  Fetcher fetcher();
}
