package org.astraea.admin;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Admin extends Closeable {

  static Builder builder() {
    return new Builder();
  }

  static Admin of(String bootstrapServers) {
    return builder().bootstrapServers(bootstrapServers).build();
  }

  static Admin of(Map<String, String> configs) {
    return builder().configs(configs).build();
  }

  /** @return names of all topics */
  Set<String> topicNames();

  /** @return the topic name and its configurations. */
  Map<String, Config> topics();

  /** @return all partitions */
  default Set<TopicPartition> partitions() {
    return partitions(topicNames());
  }

  /**
   * @param topics target
   * @return the partitions belong to input topics
   */
  Set<TopicPartition> partitions(Set<String> topics);

  /** @return a topic creator to set all topic configs and then run the procedure. */
  Creator creator();

  /** @return offsets of all partitions */
  default Map<TopicPartition, Offset> offsets() {
    return offsets(topicNames());
  }

  /**
   * @param topics topic names
   * @return the earliest offset and latest offset for specific topics
   */
  Map<TopicPartition, Offset> offsets(Set<String> topics);

  /** @return all consumer groups */
  default Map<String, ConsumerGroup> consumerGroups() {
    return consumerGroups(consumerGroupIds());
  }

  /** @return all consumer group ids */
  Set<String> consumerGroupIds();

  /**
   * @param consumerGroupNames consumer group names.
   * @return the member info of each consumer group
   */
  Map<String, ConsumerGroup> consumerGroups(Set<String> consumerGroupNames);

  /** @return replica info of all partitions */
  default Map<TopicPartition, List<Replica>> replicas() {
    return replicas(topicNames());
  }

  /**
   * @param topics topic names
   * @return the replicas of partition
   */
  Map<TopicPartition, List<Replica>> replicas(Set<String> topics);

  /** @return all broker id and their configuration */
  default Map<Integer, Config> brokers() {
    return brokers(brokerIds());
  }

  /**
   * @param brokerIds to search
   * @return broker information
   */
  Map<Integer, Config> brokers(Set<Integer> brokerIds);

  /** @return all brokers' ids */
  Set<Integer> brokerIds();

  /**
   * @param topics topic names
   * @param brokersID brokers ID
   * @return the partitions of brokers
   */
  Set<TopicPartition> partitionsOfBrokers(Set<String> topics, Set<Integer> brokersID);

  /** @return data folders of all broker nodes */
  default Map<Integer, Set<String>> brokerFolders() {
    return brokerFolders(brokerIds());
  }

  /**
   * @param brokers a Set containing broker's ID
   * @return all log directory
   */
  Map<Integer, Set<String>> brokerFolders(Set<Integer> brokers);

  /** @return a partition migrator used to move partitions to another broker or folder. */
  Migrator migrator();

  /** @return producer states of all topic partitions */
  default Map<TopicPartition, Collection<ProducerState>> producerStates() {
    return producerStates(partitions());
  }

  /**
   * @param partitions to search
   * @return producer states of input topic partitions
   */
  Map<TopicPartition, Collection<ProducerState>> producerStates(Set<TopicPartition> partitions);

  @Override
  void close();
}
