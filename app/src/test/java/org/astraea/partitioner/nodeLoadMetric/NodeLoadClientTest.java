package org.astraea.partitioner.nodeLoadMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    NodeMetadata nodeMetadata0 = new NodeMetadata("0", nodeMetrics);
    NodeMetadata nodeMetadata1 = new NodeMetadata("1", nodeMetrics);
    NodeMetadata nodeMetadata2 = new NodeMetadata("2", nodeMetrics);
    NodeMetadata nodeMetadata3 = new NodeMetadata("3", nodeMetrics);

    nodeMetadataCollection.add(nodeMetadata0);
    nodeMetadataCollection.add(nodeMetadata1);
    nodeMetadataCollection.add(nodeMetadata2);
    nodeMetadataCollection.add(nodeMetadata3);
  }

  @Test
  public void testGetBinOneCount() {
    NodeLoadClient nodeLoadClient = mock(NodeLoadClient.class);
    when(nodeLoadClient.getBinOneCount(anyInt())).thenCallRealMethod();
    assertEquals(nodeLoadClient.getBinOneCount(7), 3);
    assertEquals(nodeLoadClient.getBinOneCount(10), 2);
  }
}
