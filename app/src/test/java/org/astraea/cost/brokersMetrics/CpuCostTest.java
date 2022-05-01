package org.astraea.cost.brokersMetrics;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.FakeClusterInfo;
import org.astraea.cost.NodeInfo;
import org.astraea.cost.ReplicaInfo;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.java.OperatingSystemInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CpuCostTest {
  @Test
  void testCost() throws InterruptedException {
    var cpuUsage1 = mockResult(0.5);
    var cpuUsage2 = mockResult(0.81);
    var cpuUsage3 = mockResult(0.62);

    Collection<HasBeanObject> broker1 = List.of(cpuUsage1);
    Collection<HasBeanObject> broker2 = List.of(cpuUsage2);
    Collection<HasBeanObject> broker3 = List.of(cpuUsage3);
    ClusterInfo clusterInfo =
        new FakeClusterInfo() {
          @Override
          public Map<Integer, Collection<HasBeanObject>> allBeans() {
            return Map.of(1, broker1, 2, broker2, 3, broker3);
          }

          @Override
          public Set<String> topics() {
            return Set.of("t");
          }

          @Override
          public List<ReplicaInfo> availablePartitions(String topic) {
            return List.of(
                ReplicaInfo.of(
                    "t", 0, NodeInfo.of(1, "host1", 9092), List.of(), List.of(), List.of()),
                ReplicaInfo.of(
                    "t", 0, NodeInfo.of(2, "host2", 9092), List.of(), List.of(), List.of()),
                ReplicaInfo.of(
                    "t", 0, NodeInfo.of(3, "host3", 9092), List.of(), List.of(), List.of()));
          }
        };

    var cpuCost = new CpuCost();
    var scores = cpuCost.brokerCost(clusterInfo).value();
    Assertions.assertEquals(0.39, scores.get(1));
    Assertions.assertEquals(0.63, scores.get(2));
    Assertions.assertEquals(0.48, scores.get(3));

    Thread.sleep(1000);
    cpuUsage1 = mockResult(0.8);
    cpuUsage2 = mockResult(0.5);
    cpuUsage3 = mockResult(0.3);

    Collection<HasBeanObject> broker12 = List.of(cpuUsage1);
    Collection<HasBeanObject> broker22 = List.of(cpuUsage2);
    Collection<HasBeanObject> broker32 = List.of(cpuUsage3);
    ClusterInfo clusterInfo2 =
        new FakeClusterInfo() {
          @Override
          public Map<Integer, Collection<HasBeanObject>> allBeans() {
            return Map.of(1, broker12, 2, broker22, 3, broker32);
          }

          @Override
          public Set<String> topics() {
            return Set.of("t");
          }

          @Override
          public List<ReplicaInfo> availablePartitions(String topic) {
            return List.of(
                ReplicaInfo.of(
                    "t", 0, NodeInfo.of(1, "host1", 9092), List.of(), List.of(), List.of()),
                ReplicaInfo.of(
                    "t", 0, NodeInfo.of(2, "host2", 9092), List.of(), List.of(), List.of()),
                ReplicaInfo.of(
                    "t", 0, NodeInfo.of(3, "host3", 9092), List.of(), List.of(), List.of()));
          }
        };
    scores = cpuCost.brokerCost(clusterInfo2).value();
    Assertions.assertEquals(0.51, scores.get(1));
    Assertions.assertEquals(0.55, scores.get(2));
    Assertions.assertEquals(0.44, scores.get(3));
  }

  private OperatingSystemInfo mockResult(double usage) {
    var cpu = Mockito.mock(OperatingSystemInfo.class);
    Mockito.when(cpu.systemCpuLoad()).thenReturn(usage);
    return cpu;
  }
}
