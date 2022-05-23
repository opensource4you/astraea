package org.astraea.balancer.generator;

import java.util.stream.Stream;
import org.astraea.balancer.ClusterLogAllocation;
import org.astraea.balancer.RebalancePlanProposal;
import org.astraea.cost.ClusterInfo;

/** */
public interface RebalancePlanGenerator {

  /**
   * Generate a rebalance proposal, noted that this function doesn't require proposing exactly the
   * same plan for the same input argument. There can be some randomization that takes part in this
   * process.
   *
   * @param clusterInfo the cluster state
   * @return a {@link Stream} generating rebalance plan regarding the given {@link ClusterInfo}
   */
  default Stream<RebalancePlanProposal> generate(ClusterInfo clusterInfo) {
    return generate(clusterInfo, ClusterLogAllocation.of(clusterInfo));
  }

  /**
   * Generate a rebalance proposal, noted that this function doesn't require proposing exactly the
   * same plan for the same input argument. There can be some randomization that takes part in this
   * process.
   *
   * @param clusterInfo the cluster state, implementation can take advantage of the data inside to
   *     proposal the plan it feels confident to improve the cluster.
   * @param baseAllocation the cluster log allocation as the based of proposal generation, the given
   *     instance is considered safe to modify. No need to make an extra copy.
   * @return a {@link Stream} generating rebalance plan regarding the given {@link ClusterInfo}
   */
  Stream<RebalancePlanProposal> generate(
      ClusterInfo clusterInfo, ClusterLogAllocation baseAllocation);
}
