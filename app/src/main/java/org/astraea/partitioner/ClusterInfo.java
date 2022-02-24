package org.astraea.partitioner;

import java.util.List;
import java.util.stream.Collectors;

public interface ClusterInfo {

  static ClusterInfo of(org.apache.kafka.common.Cluster cluster) {
    return new ClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return cluster.nodes().stream().map(NodeInfo::of).collect(Collectors.toUnmodifiableList());
      }

      @Override
      public List<PartitionInfo> availablePartitions(String topic) {
        return cluster.availablePartitionsForTopic(topic).stream()
            .map(PartitionInfo::of)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public List<PartitionInfo> partitions(String topic) {
        return cluster.partitionsForTopic(topic).stream()
            .map(PartitionInfo::of)
            .collect(Collectors.toUnmodifiableList());
      }
    };
  }

  /** @return The known set of nodes */
  List<NodeInfo> nodes();

  /**
   * Get the list of available partitions for this topic
   *
   * @param topic The topic name
   * @return A list of partitions
   */
  List<PartitionInfo> availablePartitions(String topic);

  /**
   * Get the list of partitions for this topic
   *
   * @param topic The topic name
   * @return A list of partitions
   */
  List<PartitionInfo> partitions(String topic);
}
