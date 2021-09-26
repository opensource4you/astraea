package org.astraea.partitioner.nodeLoadMetric;

import static org.astraea.partitioner.nodeLoadMetric.NodeLoadClient.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import org.junit.jupiter.api.Test;

public class NodeLoadClientTest {

  @Test
  public void testGetBinOneCount() {
    NodeLoadClient nodeLoadClient = new NodeLoadClient();

    assertEquals(nodeLoadClient.getBinOneCount(7), 3);
    assertEquals(nodeLoadClient.getBinOneCount(10), 2);
  }

  @Test
  public void testTearDownClient() throws InterruptedException {
    HashMap<Integer, Double> testBrokerMsg = new HashMap<>();
    testBrokerMsg.put(0, 500.0);
    testBrokerMsg.put(1, 1500.0);
    testBrokerMsg.put(2, 800.0);
    testBrokerMsg.put(3, 1200.0);

    OverLoadNode overLoadNode = new OverLoadNode();
    overLoadNode.setEachBrokerMsgPerSec(testBrokerMsg);
    setOverLoadNode(overLoadNode);

    NodeLoadClient nodeLoadClient = getNodeLoadInstance();
    Thread.sleep(15000);
    assertEquals(getTimeOutCount(), 0);

    nodeLoadClient = getNodeLoadInstance();
    Thread.sleep(5000);
    nodeLoadClient.tellAlive();
    Thread.sleep(6000);
    assertEquals(getTimeOutCount(), 5);
  }

  @Test
  // TODO
  public void testGetInstance() throws InterruptedException {
    HashMap<Integer, Double> testBrokerMsg = new HashMap<>();
    testBrokerMsg.put(0, 500.0);
    testBrokerMsg.put(1, 1500.0);
    testBrokerMsg.put(2, 800.0);
    testBrokerMsg.put(3, 1200.0);

    OverLoadNode overLoadNode = new OverLoadNode();
    overLoadNode.setEachBrokerMsgPerSec(testBrokerMsg);
    setOverLoadNode(overLoadNode);

    NodeLoadClient nodeLoadClient = getNodeLoadInstance();
    Thread.sleep(5000);
  }
}
