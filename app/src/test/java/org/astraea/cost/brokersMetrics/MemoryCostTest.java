package org.astraea.cost.brokersMetrics;

import java.lang.management.MemoryUsage;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.FakeClusterInfo;
import org.astraea.cost.NodeInfo;
import org.astraea.cost.PartitionInfo;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.java.HasJvmMemory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MemoryCostTest {
  @Test
  void testCost() throws InterruptedException {
    var jvmMemory1 = mockResult(50L, 100L);
    var jvmMemory2 = mockResult(81L, 100L);
    var jvmMemory3 = mockResult(80L, 150L);

    Collection<HasBeanObject> broker1 = List.of(jvmMemory1);
    Collection<HasBeanObject> broker2 = List.of(jvmMemory2);
    Collection<HasBeanObject> broker3 = List.of(jvmMemory3);
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
          public List<PartitionInfo> availablePartitions(String topic) {
            return List.of(
                PartitionInfo.of(
                    "t", 0, NodeInfo.of(1, "host1", 9092), List.of(), List.of(), List.of()),
                PartitionInfo.of(
                    "t", 0, NodeInfo.of(2, "host2", 9092), List.of(), List.of(), List.of()),
                PartitionInfo.of(
                    "t", 0, NodeInfo.of(3, "host3", 9092), List.of(), List.of(), List.of()));
          }
        };

    var memoryCost = new MemoryCost();
    var scores = memoryCost.brokerCost(clusterInfo).value();
    Assertions.assertEquals(0.42, scores.get(1));
    Assertions.assertEquals(0.64, scores.get(2));
    Assertions.assertEquals(0.44, scores.get(3));

    Thread.sleep(1000);
    jvmMemory1 = mockResult(50L, 50L);
    jvmMemory2 = mockResult(30L, 100L);
    jvmMemory3 = mockResult(80L, 150L);

    Collection<HasBeanObject> broker12 = List.of(jvmMemory1);
    Collection<HasBeanObject> broker22 = List.of(jvmMemory2);
    Collection<HasBeanObject> broker32 = List.of(jvmMemory3);
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
          public List<PartitionInfo> availablePartitions(String topic) {
            return List.of(
                PartitionInfo.of(
                    "t", 0, NodeInfo.of(1, "host1", 9092), List.of(), List.of(), List.of()),
                PartitionInfo.of(
                    "t", 0, NodeInfo.of(2, "host2", 9092), List.of(), List.of(), List.of()),
                PartitionInfo.of(
                    "t", 0, NodeInfo.of(3, "host3", 9092), List.of(), List.of(), List.of()));
          }
        };
    scores = memoryCost.brokerCost(clusterInfo2).value();
    Assertions.assertEquals(0.52, scores.get(1));
    Assertions.assertEquals(0.52, scores.get(2));
    Assertions.assertEquals(0.45, scores.get(3));
  }

  private HasJvmMemory mockResult(long used, long max) {
    var jvmMemory = Mockito.mock(HasJvmMemory.class);
    var memoryUsage = Mockito.mock(MemoryUsage.class);
    Mockito.when(jvmMemory.heapMemoryUsage()).thenReturn(memoryUsage);
    Mockito.when(memoryUsage.getUsed()).thenReturn(used);
    Mockito.when(memoryUsage.getMax()).thenReturn(max);
    return jvmMemory;
  }
}
