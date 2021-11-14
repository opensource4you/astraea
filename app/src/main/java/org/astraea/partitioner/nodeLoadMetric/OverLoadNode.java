package org.astraea.partitioner.nodeLoadMetric;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class OverLoadNode {
  private double standardDeviation = 0;
  private double avgBrokersMsgPerSec = 0;
  private Collection<String> nodesID;
  private int nodeNum;
  private int mountCount = 0;
  private HashMap<String, Double> eachBrokerMsgPerSec = new HashMap();
  private Collection<NodeMetadata> nodesMetadata;

  OverLoadNode(Collection<NodeMetadata> nodeMetadataCollection) {
    this.nodesID =
        nodeMetadataCollection.stream()
            .map(NodeMetadata::getNodeID)
            .collect(Collectors.toUnmodifiableList());
    this.nodeNum = nodeMetadataCollection.size();
    this.nodesMetadata = nodeMetadataCollection;
  }

  /** Monitor and update the number of overloads of each node. */
  public void monitorOverLoad() {
    setBrokersMsgPerSec();
    setAvgBrokersMsgPerSec();
    standardDeviationImperative();
    for (NodeMetadata nodeMetadata : nodesMetadata) {
      int ifOverLoad = 0;
      if (nodeMetadata.getTotalBytes() > (avgBrokersMsgPerSec + standardDeviation)) {
        ifOverLoad = 1;
      }
      nodeMetadata.setOverLoadCount(
          setOverLoadCount(nodeMetadata.getOverLoadCount(), mountCount % 10, ifOverLoad));
    }
    mountCount++;
  }

  /**
   * Use bit operations to record whether the node exceeds the load per second,the position of the
   * number represents the recorded time.
   */
  public int setOverLoadCount(int overLoadCount, int roundCount, int ifOverLoad) {
    int x = overLoadCount & 1 << roundCount;
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

  public void setBrokersMsgPerSec() {
    for (NodeMetadata nodeMetadata : nodesMetadata) {
      eachBrokerMsgPerSec.put(nodeMetadata.getNodeID(), nodeMetadata.getTotalBytes());
    }
  }

  public void setAvgBrokersMsgPerSec() {
    double avg = 0;
    for (Map.Entry<String, Double> entry : eachBrokerMsgPerSec.entrySet()) {
      avg += entry.getValue();
    }
    this.avgBrokersMsgPerSec = avg / eachBrokerMsgPerSec.size();
  }

  public void standardDeviationImperative() {
    double variance = 0;
    for (Map.Entry<String, Double> entry : eachBrokerMsgPerSec.entrySet()) {
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
  public void setEachBrokerMsgPerSec(HashMap<String, Double> hashMap) {
    this.eachBrokerMsgPerSec = hashMap;
  }
  // Only for test
  double getAvgBrokersMsgPerSec() {
    return this.avgBrokersMsgPerSec;
  }

  // Only for test
  void setMountCount(int i) {
    this.mountCount = i;
  }
}
