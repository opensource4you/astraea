package org.astraea.partitioner.nodeLoadMetric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.astraea.concurrent.ThreadPool;

public class NodeLoadClient implements ThreadPool.Executor {

  private final OverLoadNode overLoadNode;
  private final Collection<NodeClient> nodeClientCollection = new ArrayList<>();
  private final LoadPoisson loadPoisson;

  public NodeLoadClient(HashMap<String, String> jmxAddresses) throws IOException {
    for (HashMap.Entry<String, String> entry : jmxAddresses.entrySet()) {
      this.nodeClientCollection.add(new NodeClient(entry.getKey(), entry.getValue()));
    }
    this.overLoadNode = new OverLoadNode(nodeClientCollection);
    loadPoisson = new LoadPoisson();
  }

  /** A thread that continuously updates metricsfor NodeLoadClient. */
  @Override
  public State execute() throws InterruptedException {
    try {
      refreshNodesMetrics();
      overLoadNode.monitorOverLoad();
      loadPoisson.allNodesPoisson(avgLoadCount(), nodeOverLoadCount());
      TimeUnit.SECONDS.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void close() {
    for (NodeClient nodeClient : nodeClientCollection) {
      nodeClient.close();
    }
  }

  public synchronized HashMap<String, Integer> nodeOverLoadCount() {
    HashMap<String, Integer> overLoadCount = new HashMap<>();
    for (NodeMetadata nodeMetadata : nodeClientCollection) {
      overLoadCount.put(nodeMetadata.nodeID(), nodeMetadata.overLoadCount());
    }
    return overLoadCount;
  }

  public synchronized int avgLoadCount() {
    var avgLoadCount = 0.0;

    for (NodeMetadata nodeMetadata : nodeClientCollection) {
      avgLoadCount += binOneCount(nodeMetadata.overLoadCount());
    }
    return nodeClientCollection.size() > 0 ? (int) avgLoadCount / nodeClientCollection.size() : 0;
  }

  /** Get the number of times a node is overloaded. */
  public static int binOneCount(int n) {
    var index = 0;
    var count = 0;
    while (n > 0) {
      var x = n & 1 << index;
      if (x != 0) {
        count++;
        n = n - (1 << index);
      }
      index++;
    }
    return count;
  }

  public void refreshNodesMetrics() {
    for (NodeClient nodeClient : nodeClientCollection) {
      nodeClient.refreshMetrics();
    }
  }

  public LoadPoisson getLoadPoisson() {
    return loadPoisson;
  }
}
