package org.astraea.partitioner.nodeLoadMetric;

import static org.astraea.partitioner.nodeLoadMetric.NodeLoadClient.getNodeLoadInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

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

  @Test
  public void testTearDownClient() throws InterruptedException, MalformedURLException {
    HashMap<String, Double> testBrokerMsg = new HashMap<>();
    testBrokerMsg.put("0", 500.0);
    testBrokerMsg.put("1", 1500.0);
    testBrokerMsg.put("2", 800.0);
    testBrokerMsg.put("3", 1200.0);

    OverLoadNode overLoadNode = new OverLoadNode(nodeMetadataCollection);

    for(NodeMetadata nodeMetadata : nodeMetadataCollection) {
      nodeMetadata.setTotalBytes(testBrokerMsg.get(nodeMetadata.getNodeID()));
    }

    try(MockedConstruction mocked = mockConstruction(NodeMetrics.class)){
      NodeLoadClient nodeLoadClient = getNodeLoadInstance(jmxAddresses);
      overLoadNode.setEachBrokerMsgPerSec(testBrokerMsg);
      nodeLoadClient.setOverLoadNode(overLoadNode);
      TimeUnit.SECONDS.sleep(15);
      assertEquals(nodeLoadClient.getTimeOutCount(), 0);

      nodeLoadClient = getNodeLoadInstance(jmxAddresses);
      TimeUnit.SECONDS.sleep(5);
      nodeLoadClient.tellAlive();
      TimeUnit.SECONDS.sleep(6);
      assertEquals(nodeLoadClient.getTimeOutCount(), 5);
    }
  }
}
