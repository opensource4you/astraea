package org.astraea.cost;

public interface TopicPartition extends Comparable<TopicPartition> {
  static TopicPartition of(String topic, int partition) {
    final int prime = 31;
    final int hash = (prime + topic.hashCode()) * prime + partition;
    return new TopicPartition() {
      @Override
      public String topic() {
        return topic;
      }

      @Override
      public int partition() {
        return partition;
      }

      @Override
      public int hashCode() {
        return hash;
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof TopicPartition)) return false;
        var topicPartition = (TopicPartition) o;
        return topicPartition.topic().equals(topic()) && topicPartition.partition() == partition();
      }

      @Override
      public int compareTo(TopicPartition topicPartition) {
        if (!this.topic().equals(topicPartition.topic())) {
          return this.topic().compareTo(topicPartition.topic());
        } else if (this.partition() != topicPartition.partition()) {
          return this.partition() - topicPartition.partition();
        } else {
          return 0;
        }
      }
    };
  }

  static TopicPartition of(org.apache.kafka.common.TopicPartition topicPartition) {
    return TopicPartition.of(topicPartition.topic(), topicPartition.partition());
  }

  String topic();

  int partition();
}
