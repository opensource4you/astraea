package org.astraea.partitioner.nodeLoadMetric;

import static org.astraea.partitioner.nodeLoadMetric.NodeLoadClient.getBinOneCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NodeLoadClientTest {
  private HashMap<String, String> jmxAddresses;
  private Collection<NodeMetadata> nodeMetadataCollection;

  @BeforeEach
  public void setUp() {
    jmxAddresses = new HashMap<>();
    jmxAddresses.put("0", "0.0.0.0");
    jmxAddresses.put("1", "0.0.0.0");
    jmxAddresses.put("2", "0.0.0.0");

    nodeMetadataCollection = new ArrayList<>();
    NodeMetrics nodeMetrics = mock(NodeMetrics.class);
    NodeMetadata nodeMetadata0 = new NodeMetadata("0");
    NodeMetadata nodeMetadata1 = new NodeMetadata("1");
    NodeMetadata nodeMetadata2 = new NodeMetadata("2");
    NodeMetadata nodeMetadata3 = new NodeMetadata("3");

    nodeMetadataCollection.add(nodeMetadata0);
    nodeMetadataCollection.add(nodeMetadata1);
    nodeMetadataCollection.add(nodeMetadata2);
    nodeMetadataCollection.add(nodeMetadata3);
  }

  @Test
  public void testGetBinOneCount() {
    assertEquals(getBinOneCount(7), 3);
    assertEquals(getBinOneCount(10), 2);
  }
}
