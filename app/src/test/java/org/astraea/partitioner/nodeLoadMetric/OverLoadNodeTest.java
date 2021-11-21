package org.astraea.partitioner.nodeLoadMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import org.astraea.service.RequireJmxServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OverLoadNodeTest extends RequireJmxServer {
  private Collection<NodeClient> nodeClientCollection;
  private Collection<NodeClient> nodeClientCollection2;

  @BeforeEach
  public void setUp() throws IOException {
    var jmxServer = jmxServiceURL().toString();
    nodeClientCollection = new ArrayList<>();
    NodeClient nodeClient0 = new NodeClient("0", jmxServer);
    NodeClient nodeClient1 = new NodeClient("1", jmxServer);
    NodeClient nodeClient2 = new NodeClient("2", jmxServer);
    NodeClient nodeClient3 = new NodeClient("3", jmxServer);

    nodeClientCollection.add(nodeClient0);
    nodeClientCollection.add(nodeClient1);
    nodeClientCollection.add(nodeClient2);
    nodeClientCollection.add(nodeClient3);

    nodeClientCollection2 = new ArrayList<>();
    NodeClient nodeClient10 = new NodeClient("0", jmxServer);
    NodeClient nodeClient11 = new NodeClient("1", jmxServer);
    NodeClient nodeClient12 = new NodeClient("2", jmxServer);
    NodeClient nodeClient13 = new NodeClient("3", jmxServer);

    nodeClientCollection2.add(nodeClient10);
    nodeClientCollection2.add(nodeClient11);
    nodeClientCollection2.add(nodeClient12);
    nodeClientCollection2.add(nodeClient13);
  }

  @Test
  public void testStandardDeviationImperative() {
    HashMap<String, Double> testHashMap = new HashMap<>();
    testHashMap.put("0", 10.0);
    testHashMap.put("1", 10.0);
    testHashMap.put("2", 20.0);
    testHashMap.put("3", 20.0);

    OverLoadNode overLoadNode = new OverLoadNode(nodeClientCollection);
    var testAvg = overLoadNode.setAvgBrokersMsgPerSec(testHashMap);
    assertEquals(testAvg, 15);

    overLoadNode.standardDeviationImperative(testHashMap, testAvg);

    assertEquals(overLoadNode.getStandardDeviation(), 5);
  }

  @Test
  public void testSetOverLoadCount() {
    Collection<NodeClient> nodeClientCollection = new ArrayList<NodeClient>();

    OverLoadNode overLoadNode = new OverLoadNode(nodeClientCollection);
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

    OverLoadNode overLoadNode = new OverLoadNode(nodeClientCollection);

    for (NodeClient nodeClient : nodeClientCollection) {
      nodeClient.setTotalBytes(testHashMap.get(nodeClient.getNodeID()));
    }

    for (NodeClient nodeClient : nodeClientCollection2) {
      nodeClient.setTotalBytes(testHashMap.get(nodeClient.getNodeID()));
    }

    for (int i = 0; i < 5; i++) {
      overLoadNode.setMountCount(i);
      overLoadNode.monitorOverLoad();
    }

    assertEquals(((NodeClient) nodeClientCollection.toArray()[3]).getOverLoadCount(), 31);

    for (int i = 0; i < 15; i++) {
      overLoadNode.setMountCount(i);
      overLoadNode.monitorOverLoad();
    }

    assertEquals(((NodeClient) nodeClientCollection.toArray()[3]).getOverLoadCount(), 1023);

    overLoadNode = new OverLoadNode(nodeClientCollection2);

    for (int i = 0; i < 10; i++) {
      overLoadNode.setMountCount(i);
      overLoadNode.monitorOverLoad();
    }

    assertEquals(
        ((NodeClient) nodeClientCollection.toArray()[3]).getOverLoadCount(),
        ((NodeClient) nodeClientCollection2.toArray()[3]).getOverLoadCount());
  }
}
