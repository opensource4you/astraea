package org.astraea.consumer;

import java.util.Collection;
import org.apache.kafka.common.TopicPartition;

@FunctionalInterface
public interface ConsumerRebalanceListener extends org.apache.kafka.clients.consumer.ConsumerRebalanceListener {
  @Override
  default void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
}
