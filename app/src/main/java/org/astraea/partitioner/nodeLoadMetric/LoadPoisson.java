package org.astraea.partitioner.nodeLoadMetric;

import static org.astraea.partitioner.nodeLoadMetric.NodeLoadClient.binOneCount;

import java.util.HashMap;
import java.util.Map;

public class LoadPoisson {
  private HashMap<String, Double> allPoissonMap = new HashMap<>();

  public synchronized void allNodesPoisson(
      int avgLoad, HashMap<String, Integer> nodeOverLoadCount) {
    for (Map.Entry<String, Integer> entry : nodeOverLoadCount.entrySet()) {
      var x = binOneCount(entry.getValue());
      allPoissonMap.put(entry.getKey(), doPoisson(avgLoad, x));
    }
  }

  public double doPoisson(int lambda, int x) {
    var Probability = 0.0;
    var ans = 0.0;

    for (int i = 0; i <= x; i++) {
      var j = Math.pow(lambda, i);
      var e = Math.exp(-lambda);
      var h = factorial(i);
      Probability = (j * e) / h;
      ans += Probability;
    }

    return ans;
  }

  public long factorial(long number) {
    if (number <= 1) return 1;
    else return number * factorial(number - 1);
  }

  public HashMap<String, Double> getAllPoissonMap() {
    return allPoissonMap;
  }
}
