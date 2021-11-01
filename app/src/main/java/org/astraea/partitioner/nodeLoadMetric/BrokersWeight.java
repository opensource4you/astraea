package org.astraea.partitioner.nodeLoadMetric;

import java.util.HashMap;
import java.util.Map;

public class BrokersWeight {

  /**
   * Record the current weight of each node according to Poisson calculation and the weight after
   * partitioner calculation.
   */
  private static HashMap<String, int[]> brokerHashMap = new HashMap<>();

  private LoadPoisson loadPoisson;

  public BrokersWeight(LoadPoisson loadPoisson) {
    this.loadPoisson = loadPoisson;
  }

  /** Change the weight of the node according to the current Poisson. */
  public synchronized void setBrokerHashMap() {
    HashMap<String, Double> poissonMap = loadPoisson.setAllPoisson();

    for (Map.Entry<String, Double> entry : poissonMap.entrySet()) {
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
    for (Map.Entry<String, int[]> entry : brokerHashMap.entrySet()) {
      allWeight += entry.getValue()[0];
    }
    return allWeight;
  }

  public synchronized HashMap<String, int[]> getBrokerHashMap() {
    return brokerHashMap;
  }

  public synchronized void setCurrentBrokerHashMap(HashMap<String, int[]> currentBrokerHashMap) {
    brokerHashMap = currentBrokerHashMap;
  }

  // Only for test
  void setBrokerHashMapValue(String x, int y) {
    brokerHashMap.put(x, new int[] {0, y});
  }
}
