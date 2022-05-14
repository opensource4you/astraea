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

  default Set<String> topicNames() {
    return topics().keySet();
  }

  /** @return the topic name and its configurations. */
  Map<String, Config> topics();

  default Set<String> publicTopicNames() {
    return publicTopics().keySet();
  }

  /** @return the topic name and its configurations. (Without internal topics) */
  Map<String, Config> publicTopics();

  /** @return a topic creator to set all topic configs and then run the procedure. */
  Creator creator();

  /**
   * @param topics topic names
   * @return the earliest offset and latest offset for specific topics
   */
  Map<TopicPartition, Offset> offsets(Set<String> topics);

  /** @return all consumer groups */
  default Map<String, ConsumerGroup> consumerGroups() {
    return consumerGroups(Set.of());
  }

  /**
   * @param consumerGroupNames consumer group names.
   * @return the member info of each consumer group
   */
  Map<String, ConsumerGroup> consumerGroups(Set<String> consumerGroupNames);

  /**
   * @param topics topic names
   * @return the replicas of partition
   */
  Map<TopicPartition, List<Replica>> replicas(Set<String> topics);

  /** @return all broker id and their configuration */
  Map<Integer, Config> brokers();

  /** @return all brokers' ids */
  Set<Integer> brokerIds();

  /**
   * @parm partitions map of TopicPartition and target brokers
   * @return true if the leader is change successful changed
   */
  Map<TopicPartition, Boolean> changeReplicaLeader(Map<TopicPartition, Integer> partitions);

  /**
   * @param topics topic names
   * @param brokersID brokers ID
   * @return the partitions of brokers
   */
  List<TopicPartition> partitionsOfBrokers(Set<String> topics, Set<Integer> brokersID);

  /**
   * @param brokers a Set containing broker's ID
   * @return all log directory
   */
  Map<Integer, Set<String>> brokerFolders(Collection<Integer> brokers);

  /** @return a partition migrator used to move partitions to another broker or folder. */
  Migrator migrator();

  @Override
  void close();
}
