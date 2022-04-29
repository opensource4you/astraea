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
        Arrays.stream(pf.replicas())
            .map(node -> ReplicaInfo.of(NodeInfo.of(node)))
            .collect(Collectors.toUnmodifiableList()),
        Arrays.stream(pf.inSyncReplicas())
            .map(node -> ReplicaInfo.of(NodeInfo.of(node)))
            .collect(Collectors.toUnmodifiableList()),
        Arrays.stream(pf.offlineReplicas())
            .map(node -> ReplicaInfo.of(NodeInfo.of(node)))
            .collect(Collectors.toUnmodifiableList()));
  }

  static PartitionInfo of(
      String topic,
      int partition,
      NodeInfo leader,
      List<ReplicaInfo> replicas,
      List<ReplicaInfo> inSyncReplicas,
      List<ReplicaInfo> offlineReplicas) {
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
      public List<ReplicaInfo> replicas() {
        return List.copyOf(replicas);
      }

      @Override
      public List<ReplicaInfo> inSyncReplica() {
        return List.copyOf(inSyncReplicas);
      }

      @Override
      public List<ReplicaInfo> offlineReplicas() {
        return List.copyOf(offlineReplicas);
      }
    };
  }

  /** @return topic name */
  String topic();

  /** @return partition id */
  int partition();

  /** @return information of leader replica */
  NodeInfo leader();

  /** @return a list of replicas */
  List<ReplicaInfo> replicas();

  /** @return a list of in-sync replicas */
  List<ReplicaInfo> inSyncReplica();

  /** @return a list of offline replicas */
  List<ReplicaInfo> offlineReplicas();
}
