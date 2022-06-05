package org.astraea.app.cost;

public interface HasPartitionCost extends CostFunction {
  /**
   * score all nodes according to passed beans and cluster information.
   *
   * @param clusterInfo cluster information
   * @return the score of each partition. The score ranges in [0 - 1].
   */
  PartitionCost partitionCost(ClusterInfo clusterInfo);
}
