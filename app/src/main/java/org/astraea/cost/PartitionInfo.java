package org.astraea.cost;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public interface PartitionInfo {

  static PartitionInfo of(org.apache.kafka.common.PartitionInfo pf) {
    return of(
        pf.topic(),
        pf.partition(),
        NodeInfo.of(pf.leader()),
        Arrays.stream(pf.replicas()).map(NodeInfo::of).collect(Collectors.toUnmodifiableList()));
  }

  static PartitionInfo of(String topic, int partition, NodeInfo leader) {
    return new PartitionInfo() {
      @Override
      public String topic() {
        return topic;
      }

      @Override
      public int partition() {
        return partition;
      }

      @Override
      public NodeInfo leader() {
        return leader;
      }

      @Override
      public List<NodeInfo> replicas() {
        return null;
      }
    };
  }

  static PartitionInfo of(String topic, int partition, NodeInfo leader, List<NodeInfo> replicas) {
    return new PartitionInfo() {
      @Override
      public String topic() {
        return topic;
      }

      @Override
      public int partition() {
        return partition;
      }

      @Override
      public NodeInfo leader() {
        return leader;
      }

      @Override
      public List<NodeInfo> replicas() {
        return List.copyOf(replicas);
      }
    };
  }

  /** @return topic name */
  String topic();

  /** @return partition id */
  int partition();

  /** @return information of leader node */
  NodeInfo leader();

  /** @return a list of replicas */
  List<NodeInfo> replicas();
}
