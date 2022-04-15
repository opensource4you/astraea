package org.astraea.cost;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.KafkaMetrics;

public class ThroughputCost implements CostFunction {

  @Override
  public Map<TopicPartitionReplica, Double> cost(ClusterInfo clusterInfo) {
    var score = score(clusterInfo.allBeans());

    var max = score.values().stream().mapToDouble(v -> v).max().orElse(1);

    /* In partitioner, we don't care about replica. So we just return the score of the leader. */
    return clusterInfo.topics().stream()
        .map(clusterInfo::availablePartitions)
        .flatMap(Collection::stream)
        .collect(
            Collectors.toMap(
                PartitionInfo::leaderReplica, p -> score.getOrDefault(p.leader().id(), 0.0) / max));
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
