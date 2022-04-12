package org.astraea.balancer.alpha;

import org.astraea.cost.ClusterInfo;

@FunctionalInterface
public interface RebalancePlanGenerator {

  /**
   * Generate a rebalance proposal, noted that this function doesn't require proposing exactly the
   * same plan for the same input argument. There can be some randomization that takes part in this
   * process.
   *
   * @param clusterNow the cluster state
   * @return a rebalance plan
   */
  RebalancePlanProposal generate(ClusterInfo clusterNow);
}
