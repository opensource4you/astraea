package org.astraea.cost;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.HasValue;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ReplicaSizeCostTest {

  @Test
  void partitionCost() {
    Map<Integer, Integer> brokerDiskSize = new HashMap<>();
    brokerDiskSize.put(1, 1000);
    brokerDiskSize.put(2, 1000);
    brokerDiskSize.put(3, 1000);
    var loadCostFunction = new ReplicaSizeCost(brokerDiskSize);
    var broker1ReplicaLoad = loadCostFunction.partitionCost(exampleClusterInfo()).value(1);
    var broker2ReplicaLoad = loadCostFunction.partitionCost(exampleClusterInfo()).value(2);
    var broker3ReplicaLoad = loadCostFunction.partitionCost(exampleClusterInfo()).value(3);
    // broker1
    Assertions.assertEquals(
        0.85, broker1ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-1", 0)));
    Assertions.assertEquals(
        0.45, broker1ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-1", 1)));
    Assertions.assertEquals(
        0.35, broker1ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-2", 1)));
    // broker2
    Assertions.assertEquals(
        0.45, broker2ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-1", 1)));
    Assertions.assertEquals(
        0, broker2ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-2", 0)));
    // broker3
    Assertions.assertEquals(
        0.85, broker3ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-1", 0)));
    Assertions.assertEquals(
        0, broker3ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-2", 0)));
    Assertions.assertEquals(
        0.35, broker3ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-2", 1)));
  }

  @Test
  void brokerCost() {}

  private ClusterInfo exampleClusterInfo() {
    var sizeTP1_0 =
        mockResult("Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "0", 891289600);
    var sizeTP1_1 =
        mockResult("Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "1", 471859200);
    var sizeTP2_0 =
        mockResult("Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-2", "0", 0);
    var sizeTP2_1 =
        mockResult("Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-2", "1", 367001600);
    Collection<HasBeanObject> broker1 = List.of(sizeTP1_0, sizeTP1_1, sizeTP2_1);
    Collection<HasBeanObject> broker2 = List.of(sizeTP1_1, sizeTP2_0);
    Collection<HasBeanObject> broker3 = List.of(sizeTP2_1, sizeTP1_0, sizeTP2_0);
    return new FakeClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1));
      }

      @Override
      public Set<String> topics() {
        return Set.of("test-1", "test-2");
      }

      @Override
      public List<PartitionInfo> partitions(String topic) {
        if (topic.equals("test-1"))
          return List.of(
              PartitionInfo.of(
                  "test-1",
                  0,
                  null,
                  List.of(NodeInfo.of(1, "", -1), NodeInfo.of(3, "", -1)),
                  null,
                  null),
              PartitionInfo.of(
                  "test-1",
                  1,
                  null,
                  List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1)),
                  null,
                  null));
        else
          return List.of(
              PartitionInfo.of(
                  "test-2",
                  0,
                  null,
                  List.of(NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1)),
                  null,
                  null),
              PartitionInfo.of(
                  "test-2",
                  1,
                  null,
                  List.of(NodeInfo.of(1, "", -1), NodeInfo.of(3, "", -1)),
                  null,
                  null));
      }

      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return Map.of(1, broker1, 2, broker2, 3, broker3);
      }
    };
  }

  private HasValue mockResult(String type, String name, String topic, String partition, long size) {
    var result = Mockito.mock(HasValue.class);
    var bean = Mockito.mock(BeanObject.class);
    Mockito.when(result.beanObject()).thenReturn(bean);
    Mockito.when(bean.getProperties())
        .thenReturn(Map.of("name", name, "type", type, "topic", topic, "partition", partition));

    Mockito.when(result.value()).thenReturn(size);
    return result;
  }
}
