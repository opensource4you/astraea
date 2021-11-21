package org.astraea.partitioner.nodeLoadMetric;

import java.util.HashMap;
import java.util.Map;

public class BrokersWeight {

  /**
   * Record the current weight of each node according to Poisson calculation and the weight after
   * partitioner calculation.
   */
  private static HashMap<String, int[]> brokerWeightHashMap = new HashMap<>();

  private LoadPoisson loadPoisson;

  public BrokersWeight(LoadPoisson loadPoisson) {
    this.loadPoisson = loadPoisson;
  }

  /** Change the weight of the node according to the current Poisson. */
  public synchronized void setBrokerWeightHashMap() {
    HashMap<String, Double> poissonMap = loadPoisson.getAllPoissonMap();

    for (Map.Entry<String, Double> entry : poissonMap.entrySet()) {
      if (!brokerWeightHashMap.containsKey(entry.getKey())) {
        brokerWeightHashMap.put(entry.getKey(), new int[] {(int) ((1 - entry.getValue()) * 20), 0});
      } else {
        brokerWeightHashMap.put(
            entry.getKey(),
            new int[] {
              (int) ((1 - entry.getValue()) * 20), brokerWeightHashMap.get(entry.getKey())[1]
            });
      }
    }
  }

  public synchronized int getAllWeight() {
    var allWeight = 0;
    for (Map.Entry<String, int[]> entry : brokerWeightHashMap.entrySet()) {
      allWeight += entry.getValue()[0];
    }
    return allWeight;
  }

  public synchronized HashMap<String, int[]> getBrokerHashMap() {
    return brokerWeightHashMap;
  }

  public synchronized void setCurrentBrokerHashMap(HashMap<String, int[]> currentBrokerHashMap) {
    brokerWeightHashMap = currentBrokerHashMap;
  }

  // Only for test
  void setBrokerHashMapValue(String x, int y) {
    brokerWeightHashMap.put(x, new int[] {0, y});
  }
}
