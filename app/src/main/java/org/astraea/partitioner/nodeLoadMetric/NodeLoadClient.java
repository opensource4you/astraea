package org.astraea.partitioner.nodeLoadMetric;

import java.util.*;

public class NodeLoadClient implements Runnable {

  /** This value records the number of times each node has been overloaded within ten seconds. */
  private static HashMap<Integer, Integer> overLoadCount = new HashMap();

  private static class NodeLoadClientHolder {
    private static OverLoadNode overLoadNode = new OverLoadNode();
    private static NodeLoadClient nodeLoadClient = new NodeLoadClient();
    private static Boolean clientOn = false;
    private static Boolean timeOut = false;
    private static Boolean currentAlive = false;
    private static int timeOutCount = 0;
  }

  public static NodeLoadClient getNodeLoadInstance() throws InterruptedException {
    if (!NodeLoadClientHolder.clientOn) {
      NodeLoadClientHolder.clientOn = true;
      for (int i = 0; i < 10; i++) {
        overLoadCount.put(i, 0);
      }
      Thread loadThread = new Thread(NodeLoadClientHolder.nodeLoadClient);
      loadThread.start();
    }
    return NodeLoadClientHolder.nodeLoadClient;
  }

  /** A Cradle system for NodeLoadClient. */
  @Override
  public void run() {
    try {
      while (!NodeLoadClientHolder.timeOut) {
        NodeLoadClientHolder.overLoadNode.monitorOverLoad(overLoadCount);
        NodeLoadClientHolder.timeOutCount++;
        NodeLoadClientHolder.timeOut = NodeLoadClientHolder.timeOutCount > 9;
        NodeLoadClientHolder.timeOutCount =
            NodeLoadClientHolder.currentAlive ? 0 : NodeLoadClientHolder.timeOutCount;
        NodeLoadClientHolder.currentAlive = false;
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      NodeLoadClientHolder.timeOutCount = 0;
      NodeLoadClientHolder.timeOut = false;
      NodeLoadClientHolder.clientOn = false;
    }
  }

  public static void tearDownClient() {
    NodeLoadClientHolder.clientOn = false;
  }

  public HashMap<Integer, Integer> getOverLoadCount() {
    return this.overLoadCount;
  }

  public int getAvgLoadCount() {
    double avgLoadCount = 0;
    for (Map.Entry<Integer, Integer> entry : overLoadCount.entrySet()) {
      avgLoadCount += getBinOneCount(entry.getValue());
    }
    return overLoadCount.size() > 0 ? (int) avgLoadCount / overLoadCount.size() : 0;
  }

  /** Get the number of times a node is overloaded. */
  public int getBinOneCount(int n) {
    int index = 0;
    int count = 0;
    while (n > 0) {
      int x = n & 1 << index;
      if (x != 0) {
        count++;
        n = n - (1 << index);
      }
      index++;
    }
    return count;
  }

  /** Baby don't cry. */
  public void tellAlive() {
    NodeLoadClientHolder.currentAlive = true;
  }

  // Only for test
  void setOverLoadCount(HashMap<Integer, Integer> loadCount) {
    overLoadCount = loadCount;
  }

  // Only for test
  static void setOverLoadNode(OverLoadNode loadNode) {
    NodeLoadClientHolder.overLoadNode = loadNode;
  }

  // Only for test
  static int getTimeOutCount() {
    return NodeLoadClientHolder.timeOutCount;
  }
  // TODO
  private Collection<Integer> getNodeID() {
    return null;
  }
  // TODO
  private Integer getNodeOverLoadCount(Integer nodeID) {
    return 0;
  }
  // TODO
  private static int[] getNodesID() {
    return null;
  }
}
