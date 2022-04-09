package org.astraea.balancer.alpha.cost;

import java.util.Map;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.cost.ClusterInfo;
import org.astraea.metrics.collector.Fetcher;

public class NetworkCost implements CostFunction {

  @Override
  public Map<TopicPartitionReplica, Double> cost(ClusterInfo clusterInfo) {

    return null;
  }

  @Override
  public Fetcher fetcher() {
    return null;
  }
}
