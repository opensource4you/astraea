package org.astraea.cost;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Cluster;
import org.astraea.metrics.HasBeanObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ClusterInfoTest {

  @Test
  void testNode() {
    var node = NodeInfoTest.node();
    var partition = ReplicaInfoTest.partitionInfo();
    var kafkaCluster = Mockito.mock(Cluster.class);
    Mockito.when(kafkaCluster.availablePartitionsForTopic(partition.topic()))
        .thenReturn(List.of(partition));
    Mockito.when(kafkaCluster.partitionsForTopic(partition.topic())).thenReturn(List.of(partition));
    Mockito.when(kafkaCluster.nodes()).thenReturn(List.of(node));

    var clusterInfo = ClusterInfo.of(kafkaCluster);

    Assertions.assertEquals(1, clusterInfo.nodes().size());
    Assertions.assertEquals(NodeInfo.of(node), clusterInfo.nodes().get(0));
    Assertions.assertEquals(clusterInfo.nodes().get(0), clusterInfo.node(node.host(), node.port()));
    Assertions.assertEquals(1, clusterInfo.availablePartitions(partition.topic()).size());
    Assertions.assertEquals(1, clusterInfo.partitions(partition.topic()).size());
    Assertions.assertEquals(
        NodeInfo.of(node), clusterInfo.availablePartitions(partition.topic()).get(0).nodeInfo());
    Assertions.assertEquals(
        NodeInfo.of(node),
        clusterInfo.availablePartitionLeaders(partition.topic()).get(0).nodeInfo());
    Assertions.assertEquals(
        NodeInfo.of(node), clusterInfo.partitions(partition.topic()).get(0).nodeInfo());
  }

  @Test
  void testEmptyBeans() {
    var clusterInfo = ClusterInfo.of(Mockito.mock(org.apache.kafka.common.Cluster.class));
    Assertions.assertEquals(0, clusterInfo.allBeans().size());
    Assertions.assertEquals(0, clusterInfo.beans(19).size());
  }

  @Test
  void testBeans() {
    var beans = Map.of(1, (Collection<HasBeanObject>) List.of(Mockito.mock(HasBeanObject.class)));
    var origin = Mockito.mock(ClusterInfo.class);
    Mockito.when(origin.allBeans()).thenReturn(Map.of());
    var clusterInfo = ClusterInfo.of(origin, beans);
    Assertions.assertEquals(1, clusterInfo.allBeans().size());
    Assertions.assertEquals(0, clusterInfo.beans(19).size());
    Assertions.assertEquals(1, clusterInfo.beans(1).size());
  }
}
