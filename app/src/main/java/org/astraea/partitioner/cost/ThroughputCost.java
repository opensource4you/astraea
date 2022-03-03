package org.astraea.partitioner.cost;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.partitioner.ClusterInfo;
import org.astraea.partitioner.NodeInfo;

public class ThroughputCost implements CostFunction {

  @Override
  public Map<Integer, Double> cost(
      Map<Integer, List<HasBeanObject>> beans, ClusterInfo clusterInfo) {
    var score = score(beans);

    var max = score.values().stream().mapToDouble(v -> v).max().orElse(1);

    return clusterInfo.nodes().stream()
        .map(NodeInfo::id)
        .collect(Collectors.toMap(n -> n, n -> score.getOrDefault(n, 0.0D) / max));
  }

  Map<Integer, Double> score(Map<Integer, List<HasBeanObject>> beans) {
    // TODO: this implementation only consider the oneMinuteRate ...
    return beans.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    e.getValue().stream()
                        .filter(b -> b instanceof BrokerTopicMetricsResult)
                        .map(b -> (BrokerTopicMetricsResult) b)
                        .mapToDouble(BrokerTopicMetricsResult::oneMinuteRate)
                        .sum()));
  }

  @Override
  public Fetcher fetcher() {
    return client ->
        List.of(
            KafkaMetrics.BrokerTopic.BytesInPerSec.fetch(client),
            KafkaMetrics.BrokerTopic.BytesOutPerSec.fetch(client));
  }
}
