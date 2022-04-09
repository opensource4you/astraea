package org.astraea.balancer.alpha;

import org.astraea.cost.ClusterInfo;

@FunctionalInterface
public interface RebalancePlanGenerator<T> {

  RebalancePlanProposal generate(ClusterInfo clusterNow, T arguments);
}
