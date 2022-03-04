package org.astraea.partitioner;

import java.util.List;
import org.apache.kafka.common.Cluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ClusterInfoTest {

  @Test
  void testNode() {
    var node = NodeInfoTest.node();
    var partition = PartitionInfoTest.partitionInfo();
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
        NodeInfo.of(node), clusterInfo.availablePartitions(partition.topic()).get(0).leader());
    Assertions.assertEquals(
        NodeInfo.of(node), clusterInfo.partitions(partition.topic()).get(0).leader());
  }
}
