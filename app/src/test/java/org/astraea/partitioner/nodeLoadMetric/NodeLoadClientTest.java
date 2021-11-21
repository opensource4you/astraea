package org.astraea.partitioner.nodeLoadMetric;

import static org.astraea.partitioner.nodeLoadMetric.NodeLoadClient.getBinOneCount;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.astraea.service.RequireJmxServer;
import org.junit.jupiter.api.Test;

public class NodeLoadClientTest extends RequireJmxServer {
  @Test
  public void testGetBinOneCount() {
    assertEquals(getBinOneCount(7), 3);
    assertEquals(getBinOneCount(10), 2);
  }
}
