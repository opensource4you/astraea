package org.astraea.app.cost;

import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.balancer.log.ClusterLogAllocation;

public interface HasMoveCost extends CostFunction{
    ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterLogAllocation clusterLogAllocation);
}
