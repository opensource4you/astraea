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
        Arrays.stream(pf.replicas()).map(NodeInfo::of).collect(Collectors.toUnmodifiableList()),
        Arrays.stream(pf.inSyncReplicas())
            .map(NodeInfo::of)
            .collect(Collectors.toUnmodifiableList()),
        Arrays.stream(pf.offlineReplicas())
            .map(NodeInfo::of)
            .collect(Collectors.toUnmodifiableList()));
  }

  static PartitionInfo of(
      String topic,
      int partition,
      NodeInfo leader,
      List<NodeInfo> replicas,
      List<NodeInfo> inSyncReplicas,
      List<NodeInfo> offlineReplicas) {
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

      @Override
      public List<NodeInfo> inSyncReplica() {
        return List.copyOf(inSyncReplicas);
      }

      @Override
      public List<NodeInfo> offlineReplicas() {
        return List.copyOf(offlineReplicas);
      }
    };
  }

  static TopicPartitionReplica leaderReplica(PartitionInfo partitionInfo) {
    return TopicPartitionReplica.of(
        partitionInfo.topic(), partitionInfo.partition(), partitionInfo.leader().id());
  }

  /** @return topic name */
  String topic();

  /** @return partition id */
  int partition();

  /** @return information of leader node */
  NodeInfo leader();

  /** @return a list of replicas */
  List<NodeInfo> replicas();

  /** @return a list of in-sync replicas */
  List<NodeInfo> inSyncReplica();

  /** @return a list of offline replicas */
  List<NodeInfo> offlineReplicas();
}
