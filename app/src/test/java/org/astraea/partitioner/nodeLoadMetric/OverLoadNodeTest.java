package org.astraea.partitioner.nodeLoadMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OverLoadNodeTest {
  private Collection<NodeMetadata> nodeMetadataCollection;
  private Collection<NodeMetadata> nodeMetadataCollection2;

  @BeforeEach
  public void setUp() {
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

    NodeMetadata nodeMetadata20 = new NodeMetadata("0", nodeMetrics);
    NodeMetadata nodeMetadata21 = new NodeMetadata("1", nodeMetrics);
    NodeMetadata nodeMetadata22 = new NodeMetadata("2", nodeMetrics);
    NodeMetadata nodeMetadata23 = new NodeMetadata("3", nodeMetrics);

    nodeMetadataCollection2 = new ArrayList<>();
    nodeMetadataCollection2.add(nodeMetadata20);
    nodeMetadataCollection2.add(nodeMetadata21);
    nodeMetadataCollection2.add(nodeMetadata22);
    nodeMetadataCollection2.add(nodeMetadata23);
  }

  @Test
  public void testStandardDeviationImperative() {
    HashMap<String, Double> testHashMap = new HashMap<>();
    testHashMap.put("0", 10.0);
    testHashMap.put("1", 10.0);
    testHashMap.put("2", 20.0);
    testHashMap.put("3", 20.0);

    OverLoadNode overLoadNode = new OverLoadNode(nodeMetadataCollection);
    overLoadNode.setEachBrokerMsgPerSec(testHashMap);
    overLoadNode.setAvgBrokersMsgPerSec();

    assertEquals(overLoadNode.getAvgBrokersMsgPerSec(), 15);

    overLoadNode.standardDeviationImperative();

    assertEquals(overLoadNode.getStandardDeviation(), 5);
  }

  @Test
  public void testSetOverLoadCount() {
    Collection<NodeMetadata> nodeMetadataCollection = new ArrayList<>();

    OverLoadNode overLoadNode = new OverLoadNode(nodeMetadataCollection);
    assertEquals(overLoadNode.setOverLoadCount(0, 2, 1), 4);
    assertEquals(overLoadNode.setOverLoadCount(31, 2, 1), 31);
    assertEquals(overLoadNode.setOverLoadCount(31, 2, 0), 27);
    assertEquals(overLoadNode.setOverLoadCount(20, 4, 0), 4);
  }

  @Test
  public void testMonitorOverLoad() {
    HashMap<String, Double> testHashMap = new HashMap<>();
    testHashMap.put("0", 10.0);
    testHashMap.put("1", 5.0);
    testHashMap.put("2", 20.0);
    testHashMap.put("3", 50.0);

    OverLoadNode overLoadNode = new OverLoadNode(nodeMetadataCollection);

    for (NodeMetadata nodeMetadata : nodeMetadataCollection) {
      nodeMetadata.setTotalBytes(testHashMap.get(nodeMetadata.getNodeID()));
    }

    for (NodeMetadata nodeMetadata : nodeMetadataCollection2) {
      nodeMetadata.setTotalBytes(testHashMap.get(nodeMetadata.getNodeID()));
    }

    for (int i = 0; i < 20; i++) {
      overLoadNode.setMountCount(i);
      overLoadNode.monitorOverLoad(nodeMetadataCollection);
    }

    assertEquals(((NodeMetadata) nodeMetadataCollection.toArray()[3]).getOverLoadCount(), 1023);

    for (int i = 0; i < 10; i++) {
      overLoadNode.setMountCount(i);
      overLoadNode.monitorOverLoad(nodeMetadataCollection2);
    }

    assertEquals(
        ((NodeMetadata) nodeMetadataCollection.toArray()[3]).getOverLoadCount(),
        ((NodeMetadata) nodeMetadataCollection2.toArray()[3]).getOverLoadCount());
  }
}
