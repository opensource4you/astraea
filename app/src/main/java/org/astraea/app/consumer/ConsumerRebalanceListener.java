package org.astraea.app.consumer;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.app.admin.TopicPartition;

@FunctionalInterface
public interface ConsumerRebalanceListener {

  /**
   * A callback method the user can implement to provide handling of customized offsets on
   * completion of a successful partition re-assignment. See {@link
   * org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(Collection)}
   * for more detail.
   */
  void onPartitionAssigned(Set<TopicPartition> partitions);

  static org.apache.kafka.clients.consumer.ConsumerRebalanceListener of(
      ConsumerRebalanceListener listener) {
    return new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<org.apache.kafka.common.TopicPartition> ignore) {}

      @Override
      public void onPartitionsAssigned(
          Collection<org.apache.kafka.common.TopicPartition> partitions) {
        listener.onPartitionAssigned(
            partitions.stream().map(TopicPartition::from).collect(Collectors.toSet()));
      }
    };
  }
}
