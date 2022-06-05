package org.astraea.app.cost.topic;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;

public class GetPartitionInf {
  static Map<Integer, Map<TopicPartition, Integer>> getSize(Admin client) {
    Map<Integer, Map<TopicPartition, Integer>> brokerPartitionSize = new HashMap<>();
    client
        .brokerIds()
        .forEach(
            (broker) -> {
              var partitionSize = new TreeMap<TopicPartition, Integer>();
              client
                  .replicas(client.topicNames())
                  .forEach(
                      (tp, assignment) -> {
                        assignment.forEach(
                            partition -> {
                              if (partition.broker() == broker)
                                partitionSize.put(tp, (int) partition.size());
                            });
                        brokerPartitionSize.put(broker, partitionSize);
                      });
            });
    return brokerPartitionSize;
  }

  static Map<String, Integer> getRetentionMillis(Admin client) {
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
