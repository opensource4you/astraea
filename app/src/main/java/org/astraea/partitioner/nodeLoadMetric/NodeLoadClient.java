package org.astraea.partitioner.nodeLoadMetric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.astraea.concurrent.ThreadPool;
import org.astraea.metrics.jmx.MBeanClient;

public class NodeLoadClient implements ThreadPool.Executor {

  private final OverLoadNode overLoadNode;
  private final Collection<NodeMetadata> nodeMetadataCollection = new ArrayList<>();

  public NodeLoadClient(HashMap<String, String> jmxAddresses) throws IOException {
    for (HashMap.Entry<String, String> entry : jmxAddresses.entrySet()) {
      this.nodeMetadataCollection.add(
          new NodeMetadata(entry.getKey(), createNodeMetrics(entry.getKey(), entry.getValue())));
    }
    this.overLoadNode = new OverLoadNode(this.nodeMetadataCollection);
  }

  public NodeMetrics createNodeMetrics(String key, String value) throws IOException {
    return new NodeMetrics(key, value);
  }

  /** A thread that continuously updates metricsfor NodeLoadClient. */
  @Override
  public State execute() throws InterruptedException {
    try {
      refreshNodesMetrics();
      overLoadNode.monitorOverLoad();
      TimeUnit.SECONDS.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void close() {
    for (NodeMetadata nodeMetadata : nodeMetadataCollection) {
      NodeMetrics nodeMetrics = nodeMetadata.getNodeMetrics();
      MBeanClient mBeanClient = nodeMetrics.getKafkaMetricClient();
      try {
        mBeanClient.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public synchronized HashMap<String, Integer> getAllOverLoadCount() {
    HashMap<String, Integer> overLoadCount = new HashMap<>();
    for (NodeMetadata nodeMetadata : nodeMetadataCollection) {
      overLoadCount.put(nodeMetadata.getNodeID(), nodeMetadata.getOverLoadCount());
    }
    return overLoadCount;
  }

  public synchronized int getAvgLoadCount() {
    double avgLoadCount = 0;
    for (NodeMetadata nodeMetadata : nodeMetadataCollection) {
      avgLoadCount += getBinOneCount(nodeMetadata.getOverLoadCount());
    }
    return nodeMetadataCollection.size() > 0
        ? (int) avgLoadCount / nodeMetadataCollection.size()
        : 0;
  }

  /** Get the number of times a node is overloaded. */
  public static int getBinOneCount(int n) {
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

  public Collection<NodeMetadata> getNodeMetadataCollection() {
    return this.nodeMetadataCollection;
  }
}
