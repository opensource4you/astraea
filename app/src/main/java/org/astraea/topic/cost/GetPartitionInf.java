package org.astraea.topic.cost;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.astraea.topic.TopicAdmin;

public class GetPartitionInf {
  static Map<Integer, Map<TopicPartition, Integer>> getSize(TopicAdmin client) {
    Map<Integer, Map<TopicPartition, Integer>> brokerPartitionSize = new HashMap<>();
    for (var broker : client.brokerIds()) {
      Map<TopicPartition, Integer> partitionSize = new HashMap<>();
      for (var partition : client.replicas(client.publicTopics().keySet()).keySet())
        if (client.replicas(client.publicTopics().keySet()).get(partition).get(0).broker()
            == broker)
          partitionSize.put(
              partition,
              (int) client.replicas(client.publicTopics().keySet()).get(partition).get(0).size());
      brokerPartitionSize.put(broker, partitionSize);
    }
    return brokerPartitionSize;
  }

  static Map<String, Integer> getRetentionMillis(TopicAdmin client) {
    return client.publicTopics().entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    client
                        .publicTopics()
                        .get(entry.getKey())
                        .value("retention.ms")
                        .map(Integer::parseInt)
                        .orElseThrow(
                            () -> new NoSuchElementException("retention.ms does not exist"))));
  }
}
