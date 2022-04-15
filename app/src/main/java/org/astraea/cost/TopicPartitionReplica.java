package org.astraea.cost;

public interface TopicPartitionReplica extends Comparable<TopicPartitionReplica> {
  static TopicPartitionReplica of(String topic, int partition, int brokerId) {
    final int prime = 31;
    final int hash = ((prime + topic.hashCode()) * prime + partition) * prime + brokerId;
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
        return brokerId;
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

      @Override
      public int compareTo(TopicPartitionReplica topicPartitionReplica) {
        int res = hashCode() - topicPartitionReplica.hashCode();
        if (res != 0) {
          return res;
        } else if (this.partition() != topicPartitionReplica.partition()) {
          return this.partition() - topicPartitionReplica.partition();
        } else if (this.brokerId() != topicPartitionReplica.brokerId()) {
          return this.brokerId() - topicPartitionReplica.brokerId();
        } else {
          return 0;
        }
      }
    };
  }

  static TopicPartitionReplica of(
      org.apache.kafka.common.TopicPartitionReplica topicPartitionReplica) {
    return TopicPartitionReplica.of(
        topicPartitionReplica.topic(),
        topicPartitionReplica.partition(),
        topicPartitionReplica.brokerId());
  }

  String topic();

  int partition();

  int brokerId();
}
