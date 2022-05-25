package org.astraea.cost;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.metrics.HasBeanObject;

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
  public Collection<HasBeanObject> beans(int brokerId) {
    return List.of();
  }

  @Override
  public Map<Integer, Collection<HasBeanObject>> allBeans() {
    return Map.of();
  }
}
