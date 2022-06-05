package org.astraea.cost;

public interface HasBrokerCost extends CostFunction {
  /**
   * score all nodes according to passed beans and cluster information.
   *
   * @param clusterInfo cluster information
   * @return the score of each broker. The score ranges in [0 - 1].
   */
  BrokerCost brokerCost(ClusterInfo clusterInfo);
}
