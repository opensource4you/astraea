package org.astraea.partitioner.cost;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.partitioner.ClusterInfo;
import org.astraea.partitioner.NodeInfo;

public class ThroughputCost implements CostFunction {

  @Override
  public Map<NodeInfo, Double> cost(
      Map<NodeInfo, List<HasBeanObject>> beans, ClusterInfo clusterInfo) {
    // TODO: this implementation only consider the oneMinuteRate ...
    var merged =
        beans.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        e.getValue().stream()
                            .filter(b -> b instanceof BrokerTopicMetricsResult)
                            .map(b -> (BrokerTopicMetricsResult) b)
                            .mapToDouble(BrokerTopicMetricsResult::oneMinuteRate)
                            .sum()));

    var max = merged.values().stream().mapToDouble(v -> v).max().orElse(1);

    return clusterInfo.nodes().stream()
        .collect(Collectors.toMap(n -> n, n -> merged.getOrDefault(n, 0.0D) / max));
  }

  @Override
  public Collection<Function<MBeanClient, HasBeanObject>> metricsGetters() {
    return List.of(
        KafkaMetrics.BrokerTopic.BytesInPerSec::fetch,
        KafkaMetrics.BrokerTopic.BytesOutPerSec::fetch);
  }
}
