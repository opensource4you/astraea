package org.astraea.consumer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

@FunctionalInterface
public interface ConsumerRebalanceListener {

  
  void onPartitionAssigned(Set<TopicPartition> partitions);

  static org.apache.kafka.clients.consumer.ConsumerRebalanceListener of(
      ConsumerRebalanceListener listener) {
    return new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> ignore) {}

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        listener.onPartitionAssigned(new HashSet<>(partitions));
      }
    };
  }
}
