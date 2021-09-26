package org.astraea.partitioner.nodeLoadMetric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class OverLoadNode {
  private double standardDeviation = 0;
  private double avgBrokersMsgPerSec = 0;
  private Collection<Integer> nodesID;
  private int nodeNum;
  private int mountCount = 0;
  private HashMap<Integer, Double> eachBrokerMsgPerSec = new HashMap();

  OverLoadNode() {
    this.nodesID = getNodesID();
    this.nodeNum = getNodesID().size();
  }

  /** Monitor and update the number of overloads of each node. */
  public void monitorOverLoad(HashMap<Integer, Integer> overLoadCount) {
    setBrokersMsgPerSec();
    setAvgBrokersMsgPerSec();
    standardDeviationImperative();
    for (Map.Entry<Integer, Double> entry : eachBrokerMsgPerSec.entrySet()) {
      int ifOverLoad = 0;
      if (entry.getValue() > (avgBrokersMsgPerSec + standardDeviation)) {
        ifOverLoad = 1;
      }
      overLoadCount.put(
          entry.getKey(),
          setOverLoadCount(overLoadCount.get(entry.getKey()), mountCount % 10, ifOverLoad));
    }
    this.mountCount = mountCount++;
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
    for (int nodeID : nodesID) {
      eachBrokerMsgPerSec.put(nodeID, getEachBrokerMsgPerSec(nodeID));
    }
  }

  public void setAvgBrokersMsgPerSec() {
    double avg = 0;
    for (Map.Entry<Integer, Double> entry : eachBrokerMsgPerSec.entrySet()) {
      avg += entry.getValue();
    }
    this.avgBrokersMsgPerSec = avg / nodeNum;
  }

  public void standardDeviationImperative() {
    double variance = 0;
    for (Map.Entry<Integer, Double> entry : eachBrokerMsgPerSec.entrySet()) {
      variance +=
          (entry.getValue() - avgBrokersMsgPerSec) * (entry.getValue() - avgBrokersMsgPerSec);
    }

    this.standardDeviation = Math.sqrt(variance / nodeNum);
  }

  // Only for test
  void setEachBrokerMsgPerSec(HashMap<Integer, Double> hashMap) {
    this.eachBrokerMsgPerSec = hashMap;
  }

  // Only for test
  double getStandardDeviation() {
    return this.standardDeviation;
  }

  // Only for test
  double getAvgBrokersMsgPerSec() {
    return this.avgBrokersMsgPerSec;
  }

  // Only for test
  void setMountCount(int i) {
    this.mountCount = i;
  }

  // TODO
  private double getEachBrokerMsgPerSec(int nodeID) {
    return eachBrokerMsgPerSec.get(nodeID);
  }

  // TODO
  private Collection<Integer> getNodesID() {
    Collection<Integer> testNodes = new ArrayList<Integer>();
    testNodes.add(0);
    testNodes.add(1);
    testNodes.add(2);
    testNodes.add(3);

    return testNodes;
  }
}
