package org.astraea.partitioner.nodeLoadMetric;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class NodeLoadClient implements Runnable {

  private OverLoadNode overLoadNode;
  private Collection<NodeMetadata> nodeMetadataCollection = new ArrayList<>();
  private boolean shouldDown = false;

  public NodeLoadClient(HashMap<String, String> jmxAddresses) throws MalformedURLException {
    for (Map.Entry<String, String> entry : jmxAddresses.entrySet()) {
      this.nodeMetadataCollection.add(
          new NodeMetadata(entry.getKey(), createNodeMetrics(entry.getKey(), entry.getValue())));
    }
    this.overLoadNode = new OverLoadNode(this.nodeMetadataCollection);
  }

  public NodeMetrics createNodeMetrics(String key, String value) throws MalformedURLException {
    return new NodeMetrics(key, value);
  }

  /** A thread that continuously updates metricsfor NodeLoadClient. */
  @Override
  public void run() {
    try {
      while (!shouldDown) {
        refreshNodesMetrics();
        overLoadNode.monitorOverLoad(nodeMetadataCollection);
        TimeUnit.SECONDS.sleep(1);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public HashMap<String, Integer> getAllOverLoadCount() {
    HashMap<String, Integer> overLoadCount = new HashMap<>();
    for (NodeMetadata nodeMetadata : nodeMetadataCollection) {
      overLoadCount.put(nodeMetadata.getNodeID(), nodeMetadata.getOverLoadCount());
    }
    return overLoadCount;
  }

  public int getAvgLoadCount() {
    double avgLoadCount = 0;
    for (NodeMetadata nodeMetadata : nodeMetadataCollection) {
      avgLoadCount += getBinOneCount(nodeMetadata.getOverLoadCount());
    }
    return nodeMetadataCollection.size() > 0
        ? (int) avgLoadCount / nodeMetadataCollection.size()
        : 0;
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

  public void refreshNodesMetrics() {
    for (NodeMetadata nodeMetadata : nodeMetadataCollection) {
      NodeMetrics nodeMetrics = nodeMetadata.getNodeMetrics();
      nodeMetrics.refreshMetrics();
      nodeMetadata.setTotalBytes(nodeMetrics.totalBytesPerSec());
    }
  }

  public void shouldDownNow() {
    this.shouldDown = true;
  }
}
