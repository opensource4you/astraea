package org.astraea.topic.cost;

import java.util.*;
import org.apache.kafka.common.TopicPartition;
import org.astraea.topic.TopicAdmin;

public class GetPartitionInf {
  static Map<Integer, Map<TopicPartition, Integer>> getSize(TopicAdmin client) {
    Map<Integer, Map<TopicPartition, Integer>> brokerPartitionSize = new HashMap<>();
    for (var broker : client.brokerIds()) {
      Map<TopicPartition, Integer> partitionSize = new HashMap<>();
      for (var partition : client.replicas(client.topics().keySet()).keySet())
        if (client.replicas(client.topics().keySet()).get(partition).get(0).broker() == broker)
          partitionSize.put(
              partition,
              (int) client.replicas(client.topics().keySet()).get(partition).get(0).size());
      brokerPartitionSize.put(broker, partitionSize);
    }
    return brokerPartitionSize;
  }

  static Map<String, Integer> getRetentionMillis(TopicAdmin client) {
    Map<String, Integer> retentionTime = new HashMap<>();
    for (var topic : client.topics().keySet()) {
      retentionTime.put(topic, Integer.parseInt(client.topics().get(topic).get("retention.ms")));
    }
    return retentionTime;
  }
}
