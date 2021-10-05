package org.astraea.partitioner.nodeLoadMetric;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class NodeLoadClient implements Runnable {

  private static class NodeLoadClientHolder {
    private static NodeLoadClient nodeLoadClient;
    private static boolean clientOn = false;
  }

  private OverLoadNode overLoadNode;
  private Collection<NodeMetadata> nodeMetadataCollection = new ArrayList<>();
  private int timeOutCount = 0;
  private boolean currentAlive = false;
  private boolean timeOut = false;

  NodeLoadClient(HashMap<String, String> jmxAddresses) throws MalformedURLException {
    for(Map.Entry<String, String> entry : jmxAddresses.entrySet()){
      this.nodeMetadataCollection.add(new NodeMetadata(entry.getKey(), new NodeMetrics(entry.getKey(), entry.getValue())));
    }
    this.overLoadNode = new OverLoadNode(this.nodeMetadataCollection);
    NodeLoadClientHolder.clientOn = true;
  }

  public static NodeLoadClient getNodeLoadInstance(HashMap<String, String> jmxAddresses) throws InterruptedException, MalformedURLException {
    if (!NodeLoadClientHolder.clientOn) {
      NodeLoadClientHolder.nodeLoadClient = new NodeLoadClient(jmxAddresses);
      NodeLoadClientHolder.clientOn = true;

      Thread loadThread = new Thread(NodeLoadClientHolder.nodeLoadClient);
      loadThread.start();
    }
    return NodeLoadClientHolder.nodeLoadClient;
  }

  /** A Cradle system for NodeLoadClient. */
  @Override
  public void run() {
    try {
      while (!timeOut) {
        refreshNodesMetrics();
        overLoadNode.monitorOverLoad(nodeMetadataCollection);
        timeOutCount++;
        timeOut = timeOutCount > 9;
        timeOutCount =
            currentAlive ? 0 : timeOutCount;
        currentAlive = false;
        TimeUnit.SECONDS.sleep(1);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      timeOutCount = 0;
      timeOut = false;
      NodeLoadClientHolder.clientOn = false;
      NodeLoadClientHolder.nodeLoadClient = null;
    }
  }


  public HashMap<String, Integer> getAllOverLoadCount() {
    HashMap<String, Integer> overLoadCount = new HashMap<>();
    for (NodeMetadata nodeMetadata : nodeMetadataCollection){
      overLoadCount.put(nodeMetadata.getNodeID(), nodeMetadata.getOverLoadCount());
    }
    return overLoadCount;
  }

  public int getAvgLoadCount() {
    double avgLoadCount = 0;
    for (NodeMetadata nodeMetadata : nodeMetadataCollection) {
      avgLoadCount += getBinOneCount(nodeMetadata.getOverLoadCount());
    }
    return nodeMetadataCollection.size() > 0 ? (int) avgLoadCount / nodeMetadataCollection.size() : 0;
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
  public synchronized void tellAlive() {
    currentAlive = true;
  }

  public static boolean ensureNodeLoadClientNull() {
    return NodeLoadClientHolder.nodeLoadClient == null;
  }

  public void refreshNodesMetrics() {
    for (NodeMetadata nodeMetadata : nodeMetadataCollection){
      NodeMetrics nodeMetrics = nodeMetadata.getNodeMetrics();
      nodeMetrics.refreshMetrics();
      nodeMetadata.setTotalBytes(nodeMetrics.totalBytesPerSec());
    }
  }

  // Only for test
  public void setOverLoadNode(OverLoadNode loadNode) {
    overLoadNode = loadNode;
  }

  // Only for test
  public int getTimeOutCount() {
    return timeOutCount;
  }
}
