package org.astraea.cost;

import java.util.stream.Stream;

public interface TopicPartitionReplica {
  static TopicPartitionReplica of(String topic, int partition, int brokerID) {
    final int prime = 31;
    final int hash = ((prime + topic.hashCode()) * prime + partition) * prime + brokerID;
    return new TopicPartitionReplica() {
      @Override
      public String topic() {
        return topic;
      }

      @Override
      public int partition() {
        return partition;
      }

      @Override
      public int brokerId() {
        return brokerID;
      }

      @Override
      public int hashCode() {
        return hash;
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof TopicPartitionReplica)) return false;
        var topicPartitionReplica = (TopicPartitionReplica) o;
        return topicPartitionReplica.topic().equals(topic())
            && topicPartitionReplica.partition() == partition()
            && topicPartitionReplica.brokerId() == brokerId();
      }
    };
  }

  static TopicPartitionReplica of(
      org.apache.kafka.common.TopicPartitionReplica topicPartitionReplica) {
    return new TopicPartitionReplica() {
      @Override
      public String topic() {
        return topicPartitionReplica.topic();
      }

      @Override
      public int partition() {
        return topicPartitionReplica.partition();
      }

      @Override
      public int brokerId() {
        return topicPartitionReplica.brokerId();
      }

      @Override
      public int hashCode() {
        return topicPartitionReplica.hashCode();
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof TopicPartitionReplica)) return false;
        var topicPartitionReplica = (TopicPartitionReplica) o;
        return topicPartitionReplica.topic().equals(topic())
            && topicPartitionReplica.partition() == partition()
            && topicPartitionReplica.brokerId() == brokerId();
      }
    };
  }

  static TopicPartitionReplica leaderOf(PartitionInfo partitionInfo) {
    return of(partitionInfo.topic(), partitionInfo.partition(), partitionInfo.leader().id());
  }

  static Stream<TopicPartitionReplica> streamOf(PartitionInfo partitionInfo) {
    return partitionInfo.replicas().stream()
        .map(
            node ->
                TopicPartitionReplica.of(
                    partitionInfo.topic(), partitionInfo.partition(), node.id()));
  }

  String topic();

  int partition();

  int brokerId();
}
