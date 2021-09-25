package org.astraea.partitioner.nodeLoadMetric;

import java.util.*;

public class NodeLoadClient {

  /** This value records the number of times each node has been overloaded within ten seconds. */
  private static HashMap<Integer, Integer> overLoadCount = new HashMap<Integer, Integer>();

  public static void setOverLoadCount() {
    Timer timer = new Timer();
    timer.schedule(
        new TimerTask() {
          public OverLoadNode overLoadNode = new OverLoadNode();

          public void run() {
            overLoadNode.monitorOverLoad(overLoadCount);
          }
        },
        1,
        1000);
  }

  public HashMap<Integer, Integer> getOverLoadCount() {
    return this.overLoadCount;
  }

  public int getAvgLoadCount() {
    double avgLoadCount = 0;
    for (Map.Entry<Integer, Integer> entry : overLoadCount.entrySet()) {
      avgLoadCount += getBinOneCount(entry.getValue());
    }
    return (int) avgLoadCount / overLoadCount.size();
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
