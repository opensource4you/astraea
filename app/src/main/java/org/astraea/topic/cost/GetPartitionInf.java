package org.astraea.topic.cost;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.astraea.topic.TopicAdmin;

public class GetPartitionInf {
  static Map<Integer, Map<TopicPartition, Integer>> getSize(TopicAdmin client) {
    Map<Integer, Map<TopicPartition, Integer>> brokerPartitionSize = new HashMap<>();
    client
        .brokerIds()
        .forEach(
            (broker) -> {
              TreeMap<TopicPartition, Integer> partitionSize =
                  new TreeMap<>(
                      Comparator.comparing(TopicPartition::topic)
                          .thenComparing(TopicPartition::partition));
              client
                  .replicas(client.topicNames())
                  .forEach(
                      (tp, assignment) -> {
                        if (assignment.get(0).broker() == broker)
                          partitionSize.put(tp, (int) assignment.get(0).size());
                        brokerPartitionSize.put(broker, partitionSize);
                      });
            });
    return brokerPartitionSize;
  }

  static Map<String, Integer> getRetentionMillis(TopicAdmin client) {
    return client.topics().entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    entry
                        .getValue()
                        .value("retention.ms")
                        .map(Integer::parseInt)
                        .orElseThrow(
                            () -> new NoSuchElementException("retention.ms does not exist"))));
  }
}
