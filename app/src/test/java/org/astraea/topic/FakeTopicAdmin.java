package org.astraea.topic;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

public abstract class FakeTopicAdmin implements TopicAdmin {
  @Override
  public Set<String> topics() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTopic(String topic, int numberOfPartitions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTopic(String topic, int numberOfPartitions, short replicas) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<TopicPartition, Offset> offsets(Set<String> topics) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<TopicPartition, List<Group>> groups(Set<String> topics) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<TopicPartition, List<Replica>> replicas(Set<String> topics) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Integer> brokerIds() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reassign(String topicName, int partition, Set<Integer> brokers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {}
}
