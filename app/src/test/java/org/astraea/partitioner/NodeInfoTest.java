package org.astraea.partitioner;

import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NodeInfoTest {

  static Node node() {
    return new Node(10, "host", 100);
  }

  @Test
  void testAllGetters() {
    var kafkaNode = node();
    var node = NodeInfo.of(kafkaNode);

    Assertions.assertEquals(kafkaNode.host(), node.host());
    Assertions.assertEquals(kafkaNode.id(), node.id());
    Assertions.assertEquals(kafkaNode.port(), node.port());
  }
}
