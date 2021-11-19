package org.astraea.partitioner.nodeLoadMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OverLoadNodeTest {
  private Collection<NodeMetadata> nodeMetadataCollection;
  private Collection<NodeMetadata> nodeMetadataCollection2;

  @BeforeEach
  public void setUp() {
    nodeMetadataCollection = new ArrayList<>();
    NodeMetadata nodeMetadata0 = new NodeMetadata("0");
    NodeMetadata nodeMetadata1 = new NodeMetadata("1");
    NodeMetadata nodeMetadata2 = new NodeMetadata("2");
    NodeMetadata nodeMetadata3 = new NodeMetadata("3");

    nodeMetadataCollection.add(nodeMetadata0);
    nodeMetadataCollection.add(nodeMetadata1);
    nodeMetadataCollection.add(nodeMetadata2);
    nodeMetadataCollection.add(nodeMetadata3);

    NodeMetadata nodeMetadata20 = new NodeMetadata("0");
    NodeMetadata nodeMetadata21 = new NodeMetadata("1");
    NodeMetadata nodeMetadata22 = new NodeMetadata("2");
    NodeMetadata nodeMetadata23 = new NodeMetadata("3");

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
    var testAvg = overLoadNode.setAvgBrokersMsgPerSec(testHashMap);
    assertEquals(testAvg, 15);

    overLoadNode.standardDeviationImperative(testHashMap, testAvg);

    assertEquals(overLoadNode.getStandardDeviation(), 5);
  }

  @Test
  public void testSetOverLoadCount() {
    Set<NodeMetadata> nodeMetricsCollection = new HashSet<>();

    OverLoadNode overLoadNode = new OverLoadNode(nodeMetricsCollection);
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

    for (int i = 0; i < 5; i++) {
      overLoadNode.setMountCount(i);
      overLoadNode.monitorOverLoad();
    }

    assertEquals(((NodeMetadata) nodeMetadataCollection.toArray()[3]).getOverLoadCount(), 31);

    for (int i = 0; i < 15; i++) {
      overLoadNode.setMountCount(i);
      overLoadNode.monitorOverLoad();
    }

    assertEquals(((NodeMetadata) nodeMetadataCollection.toArray()[3]).getOverLoadCount(), 1023);

    overLoadNode = new OverLoadNode(nodeMetadataCollection2);

    for (int i = 0; i < 10; i++) {
      overLoadNode.setMountCount(i);
      overLoadNode.monitorOverLoad();
    }

    assertEquals(
        ((NodeMetadata) nodeMetadataCollection.toArray()[3]).getOverLoadCount(),
        ((NodeMetadata) nodeMetadataCollection2.toArray()[3]).getOverLoadCount());
  }
}
