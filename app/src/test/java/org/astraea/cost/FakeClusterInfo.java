package org.astraea.cost;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.admin.BeansGetter;

public class FakeClusterInfo implements ClusterInfo {
  @Override
  public List<NodeInfo> nodes() {
    return List.of();
  }

  @Override
  public Set<String> dataDirectories(int brokerId) {
    return Set.of();
  }

  @Override
  public List<ReplicaInfo> availableReplicaLeaders(String topic) {
    return List.of();
  }

  @Override
  public List<ReplicaInfo> availableReplicas(String topic) {
    return List.of();
  }

  @Override
  public Set<String> topics() {
    return Set.of();
  }

  @Override
  public List<ReplicaInfo> replicas(String topic) {
    return List.of();
  }

  @Override
  public BeansGetter beans() {
    return BeansGetter.of(Map.of());
  }
}
