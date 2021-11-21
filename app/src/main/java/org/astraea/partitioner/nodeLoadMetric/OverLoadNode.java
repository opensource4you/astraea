package org.astraea.partitioner.nodeLoadMetric;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class OverLoadNode {
  private double standardDeviation = 0;
  private Collection<String> nodesID;
  private int nodeNum;
  private int mountCount = 0;
  private Collection<NodeClient> nodeClientCollection;

  OverLoadNode(Collection<NodeClient> nodeMetrics) {
    this.nodesID =
        nodeMetrics.stream().map(NodeMetadata::nodeID).collect(Collectors.toUnmodifiableList());
    this.nodeNum = nodeMetrics.size();
    this.nodeClientCollection = nodeMetrics;
  }

  /** Monitor and update the number of overloads of each node. */
  public void monitorOverLoad() {
    var eachBrokerMsgPerSec = brokersMsgPerSec();
    var avgBrokersMsgPerSec = avgBrokersMsgPerSec(eachBrokerMsgPerSec);
    standardDeviationImperative(eachBrokerMsgPerSec, avgBrokersMsgPerSec);
    for (NodeClient nodeClient : nodeClientCollection) {
      var ifOverLoad = 0;
      NodeMetadata nodeMetadata = nodeClient;
      if (nodeMetadata.totalBytes() > (avgBrokersMsgPerSec + standardDeviation)) {
        ifOverLoad = 1;
      }
      nodeClient.setOverLoadCount(
          overLoadCount(nodeMetadata.overLoadCount(), mountCount % 10, ifOverLoad));
    }
    mountCount++;
  }

  /**
   * Use bit operations to record whether the node exceeds the load per second,the position of the
   * number represents the recorded time.
   */
  public int overLoadCount(int overLoadCount, int roundCount, int ifOverLoad) {
    var x = overLoadCount & 1 << roundCount;
    if (x == ifOverLoad << roundCount) {
      return overLoadCount;
    } else {
      if (ifOverLoad != 0) {
        return overLoadCount | 1 << roundCount;
      } else {
        return overLoadCount - (int) Math.pow(2, roundCount);
      }
    }
  }

  public HashMap<String, Double> brokersMsgPerSec() {
    var eachMsg = new HashMap<String, Double>();
    for (NodeMetadata nodeMetadata : nodeClientCollection) {
      eachMsg.put(nodeMetadata.nodeID(), nodeMetadata.totalBytes());
    }
    return eachMsg;
  }

  public double avgBrokersMsgPerSec(HashMap<String, Double> eachMsg) {
    var avg = 0.0;
    for (Map.Entry<String, Double> entry : eachMsg.entrySet()) {
      avg += entry.getValue();
    }
    return avg / nodeNum;
  }

  public void standardDeviationImperative(
      HashMap<String, Double> eachMsg, double avgBrokersMsgPerSec) {
    var variance = 0.0;
    for (Map.Entry<String, Double> entry : eachMsg.entrySet()) {
      variance +=
          (entry.getValue() - avgBrokersMsgPerSec) * (entry.getValue() - avgBrokersMsgPerSec);
    }
    this.standardDeviation = Math.sqrt(variance / nodeNum);
  }

  // Only for test
  double getStandardDeviation() {
    return this.standardDeviation;
  }

  // Only for test
  void setMountCount(int i) {
    this.mountCount = i;
  }
}
