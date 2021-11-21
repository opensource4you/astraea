package org.astraea.topic;

import java.io.Closeable;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;

public interface TopicAdmin extends Closeable {

  static Builder builder() {
    return new Builder();
  }

  static TopicAdmin of(String brokers) {
    return builder().brokers(brokers).build();
  }

  static TopicAdmin of(Map<String, Object> configs) {
    return builder().configs(configs).build();
  }

  default Set<String> topicNames() {
    return topics().keySet();
  }

  /** @return the topic name and its configurations. */
  Map<String, Map<String, String>> topics();

  /** @return a topic creator to set all topic configs and then run the procedure. */
  Creator creator();

  /**
   * @param topics topic names
   * @return the earliest offset and latest offset for specific topics
   */
  Map<TopicPartition, Offset> offsets(Set<String> topics);

  /**
   * @param topics topic names
   * @return the partition having consumer group id and consumer group offset
   */
  Map<TopicPartition, List<Group>> groups(Set<String> topics);

  /**
   * @param topics topic names
   * @return the replicas of partition
   */
  Map<TopicPartition, List<Replica>> replicas(Set<String> topics);

  /**
   * @param broker broker ID
   * @return all log directory
   */
  Map<Integer, Set<String>> brokerFolders(Set<Integer> broker);

  /** @return all brokers' ids */
  Set<Integer> brokerIds();

  /**
   * Assign the topic partition to specific brokers.
   *
   * @param topicName topic name
   * @param partition partition
   * @param brokers to hold all the
   */
  void reassign(String topicName, int partition, Set<Integer> brokers);

  /**
   * @param broker broker ID
   * @param topicName topic name
   * @param partition partition
   * @param path the partition will move to
   */
  void reassignFolder(Integer broker, String topicName, Integer partition, String path);
}
