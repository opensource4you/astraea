package org.astraea.cost;

import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionInfoTest {

  static org.apache.kafka.common.PartitionInfo partitionInfo() {
    return new org.apache.kafka.common.PartitionInfo(
        "ttt", 100, NodeInfoTest.node(), new Node[0], new Node[0]);
  }

  @Test
  void testAllGetters() {
    var kafkaPartition = partitionInfo();

    var partitionInfo = PartitionInfo.of(kafkaPartition);

    Assertions.assertEquals(kafkaPartition.topic(), partitionInfo.topic());
    Assertions.assertEquals(kafkaPartition.partition(), partitionInfo.partition());
    Assertions.assertEquals(NodeInfo.of(kafkaPartition.leader()), partitionInfo.leader());
  }
}
