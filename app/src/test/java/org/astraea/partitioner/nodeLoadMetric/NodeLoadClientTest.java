package org.astraea.partitioner.nodeLoadMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class NodeLoadClientTest {
  @Test
  public void testGetBinOneCount() {
    NodeLoadClient nodeLoadClient = new NodeLoadClient();
    assertEquals(nodeLoadClient.getBinOneCount(7), 3);
  }
}
