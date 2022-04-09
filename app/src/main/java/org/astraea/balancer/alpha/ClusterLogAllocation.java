package org.astraea.balancer.alpha;

import java.util.List;
import java.util.Map;

public class ClusterLogAllocation {

  private final Map<String, Map<Integer, List<Integer>>> allocationAfterRebalance;

  public ClusterLogAllocation(Map<String, Map<Integer, List<Integer>>> allocation) {
    this.allocationAfterRebalance = Map.copyOf(allocation);
  }

  public Map<String, Map<Integer, List<Integer>>> allocation() {
    return allocationAfterRebalance;
  }
}
