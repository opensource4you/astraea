package org.astraea.consumer;

import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

@FunctionalInterface
public interface AssignedListener extends ConsumerRebalanceListener {
  @Override
  default void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

  /**
   * A callback method the user can implement to provide handling of customized offsets on
   * completion of a successful partition re-assignment. See {@link
   * ConsumerRebalanceListener#onPartitionsAssigned(Collection)} for more detail.
   */
  @Override
  void onPartitionsAssigned(Collection<TopicPartition> partitions);
}
