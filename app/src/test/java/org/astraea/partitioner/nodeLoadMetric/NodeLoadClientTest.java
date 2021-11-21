package org.astraea.partitioner.nodeLoadMetric;

import static org.astraea.partitioner.nodeLoadMetric.NodeLoadClient.binOneCount;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.astraea.service.RequireJmxServer;
import org.junit.jupiter.api.Test;

public class NodeLoadClientTest extends RequireJmxServer {
  @Test
  public void testGetBinOneCount() {
    assertEquals(binOneCount(7), 3);
    assertEquals(binOneCount(10), 2);
  }
}
