package org.astraea.partitioner.nodeLoadMetric;

import java.util.HashMap;
import java.util.Map;

public class BrokersWeight {

  /**
   * Record the current weight of each node according to Poisson calculation and the weight after
   * partitioner calculation.
   */
  private static HashMap<Integer, int[]> brokerHashMap = new HashMap<>();

  /** Change the weight of the node according to the current Poisson. */
  public synchronized void setBrokerHashMap(Map<Integer, Double> poissonMap) {
    for (Map.Entry<Integer, Double> entry : poissonMap.entrySet()) {
      if (!brokerHashMap.containsKey(entry.getKey())) {
        brokerHashMap.put(entry.getKey(), new int[] {(int) ((1 - entry.getValue()) * 20), 0});
      } else {
        brokerHashMap.put(
            entry.getKey(),
            new int[] {(int) ((1 - entry.getValue()) * 20), brokerHashMap.get(entry.getKey())[1]});
      }
    }
  }

  public synchronized int getAllWeight() {
    int allWeight = 0;
    for (Map.Entry<Integer, int[]> entry : brokerHashMap.entrySet()) {
      allWeight += entry.getValue()[0];
    }
    return allWeight;
  }

  public synchronized HashMap<Integer, int[]> getBrokerHashMap() {
    return brokerHashMap;
  }

  public synchronized void setCurrentBrokerHashMap(HashMap<Integer, int[]> currentBrokerHashMap) {
    brokerHashMap = currentBrokerHashMap;
  }

  // Only for test
  void setBrokerHashMapValue(Integer x, int y) {
    brokerHashMap.put(x, new int[] {0, y});
  }
}
