package org.astraea.admin;

import java.util.Comparator;
import java.util.Objects;

public final class TopicPartition implements Comparable<TopicPartition> {

  public static TopicPartition from(org.apache.kafka.common.TopicPartition tp) {
    return new TopicPartition(tp.topic(), tp.partition());
  }

  public static org.apache.kafka.common.TopicPartition to(TopicPartition tp) {
    return new org.apache.kafka.common.TopicPartition(tp.topic(), tp.partition());
  }

  private final int partition;
  private final String topic;

  public TopicPartition(String topic, int partition) {
    this.partition = partition;
    this.topic = topic;
  }

  @Override
  public int compareTo(TopicPartition o) {
    var r = topic.compareTo(o.topic);
    if (r != 0) return r;
    return Integer.compare(partition, o.partition);
  }

  public int partition() {
    return partition;
  }

  public String topic() {
    return topic;
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition);
  }

  @Override
  public String toString() {
    return topic + "-" + partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TopicPartition that = (TopicPartition) o;
    return partition == that.partition && Objects.equals(topic, that.topic);
  }
}
