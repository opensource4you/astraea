package org.astraea.partitioner.nodeLoadMetric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.astraea.concurrent.ThreadPool;
import org.astraea.metrics.jmx.MBeanClient;

public class NodeLoadClient implements ThreadPool.Executor {

  private final OverLoadNode overLoadNode;
  private final Collection<NodeMetrics> nodeMetricsCollection = new ArrayList<>();
  private final LoadPoisson loadPoisson;

  public NodeLoadClient(HashMap<String, String> jmxAddresses) throws IOException {
    for (HashMap.Entry<String, String> entry : jmxAddresses.entrySet()) {
      this.nodeMetricsCollection.add(new NodeMetrics(entry.getKey(), entry.getValue()));
    }
    this.overLoadNode =
        new OverLoadNode(
            this.nodeMetricsCollection.stream()
                .map(NodeMetrics::getNodeMetadata)
                .collect(Collectors.toList()));
    loadPoisson = new LoadPoisson();
  }

  /** A thread that continuously updates metricsfor NodeLoadClient. */
  @Override
  public State execute() throws InterruptedException {
    try {
      refreshNodesMetrics();
      overLoadNode.monitorOverLoad();
      loadPoisson.setAllPoisson(getAvgLoadCount(), getNodeOverLoadCount());
      TimeUnit.SECONDS.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void close() {
    for (NodeMetrics nodeMetrics : nodeMetricsCollection) {
      MBeanClient mBeanClient = nodeMetrics.getKafkaMetricClient();
      try {
        mBeanClient.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public synchronized HashMap<String, Integer> getNodeOverLoadCount() {
    HashMap<String, Integer> overLoadCount = new HashMap<>();
    for (NodeMetrics nodeMetrics : nodeMetricsCollection) {
      SafeMetadata safeMetadata = nodeMetrics.getNodeMetadata();
      overLoadCount.put(safeMetadata.getNodeID(), safeMetadata.getOverLoadCount());
    }
    return overLoadCount;
  }

  public synchronized int getAvgLoadCount() {
    double avgLoadCount = 0;

    for (NodeMetrics nodeMetrics : nodeMetricsCollection) {
      SafeMetadata safeMetadata = nodeMetrics.getNodeMetadata();
      avgLoadCount += getBinOneCount(safeMetadata.getOverLoadCount());
    }
    return nodeMetricsCollection.size() > 0 ? (int) avgLoadCount / nodeMetricsCollection.size() : 0;
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
    for (NodeMetrics nodeMetrics : nodeMetricsCollection) {
      nodeMetrics.refreshMetrics();
    }
  }

  public LoadPoisson getLoadPoisson() {
    return loadPoisson;
  }
}
