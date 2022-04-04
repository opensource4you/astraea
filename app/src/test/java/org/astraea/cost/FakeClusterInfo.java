package org.astraea.cost;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.astraea.metrics.HasBeanObject;

public class FakeClusterInfo implements ClusterInfo {
  @Override
  public List<NodeInfo> nodes() {
    return List.of();
  }

  @Override
  public List<PartitionInfo> availablePartitions(String topic) {
    return List.of();
  }

  @Override
  public List<PartitionInfo> partitions(String topic) {
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
