package org.astraea.admin;

import java.util.Objects;

public class TopicPartitionReplica implements Comparable<TopicPartitionReplica> {
  public static TopicPartitionReplica from(org.apache.kafka.common.TopicPartitionReplica tpr) {
    return new TopicPartitionReplica(tpr.topic(), tpr.partition(), tpr.brokerId());
  }

  public static org.apache.kafka.common.TopicPartitionReplica to(TopicPartitionReplica tpr) {
    return new org.apache.kafka.common.TopicPartitionReplica(
        tpr.topic, tpr.partition, tpr.brokerId());
  }

  public static TopicPartitionReplica of(String topic, String partition, int brokerId) {
    return of(brokerId + ": " + topic + "-" + partition);
  }

  public static TopicPartitionReplica of(String value) {
    var b = value.lastIndexOf(":");
    var lhs = value.lastIndexOf("-");
    if (b <= 0 || b == value.length() - 1 || lhs <= 0 || lhs == value.length() - 1)
      throw new IllegalArgumentException(
          value + " has illegal format. It should be {brokerId}:{topic}-{partition}");
    try {
      return new TopicPartitionReplica(
          value.substring(b + 1, lhs),
          Integer.parseInt(value.substring(lhs + 1)),
          Integer.parseInt(value.substring(0, b)));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("broker id and partition id must be number");
    }
  }

  private final int brokerId;
  private final int partition;
  private final String topic;

  public TopicPartitionReplica(String topic, int partition, int brokerId) {
    this.partition = partition;
    this.topic = topic;
    this.brokerId = brokerId;
  }

  @Override
  public int compareTo(TopicPartitionReplica o) {
    var t = topic.compareTo(o.topic);
    if (t != 0) return t;
    var r = Integer.compare(partition, o.partition);
    if (r != 0) return r;
    return Integer.compare(brokerId, o.brokerId);
  }

  public int brokerId() {
    return brokerId;
  }

  public int partition() {
    return partition;
  }

  public String topic() {
    return topic;
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition, brokerId);
  }

  @Override
  public String toString() {
    return brokerId + ": " + topic + "-" + partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TopicPartitionReplica that = (TopicPartitionReplica) o;
    return brokerId == that.brokerId
        && partition == that.partition
        && Objects.equals(topic, that.topic);
  }
}
