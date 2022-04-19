package org.astraea.cost;

import org.astraea.metrics.collector.Fetcher;

public interface CostFunction {

  static CostFunction throughput() {
    return new ThroughputCost();
  }

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  Fetcher fetcher();
}
