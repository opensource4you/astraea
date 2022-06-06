package org.astraea.app.cost;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.app.metrics.kafka.KafkaMetrics;

public class ThroughputCost implements HasBrokerCost {

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    var score = score(clusterInfo.allBeans());

    var max = score.values().stream().mapToDouble(v -> v).max().orElse(1);

    return () ->
        clusterInfo.nodes().stream()
            .map(NodeInfo::id)
            .collect(Collectors.toMap(n -> n, n -> score.getOrDefault(n, 0.0D) / max));
  }

  /* Score by broker. */
  Map<Integer, Double> score(Map<Integer, Collection<HasBeanObject>> beans) {
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
