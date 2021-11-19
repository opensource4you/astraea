package org.astraea.partitioner.nodeLoadMetric;

import static org.astraea.partitioner.nodeLoadMetric.NodeLoadClient.getBinOneCount;

import java.util.HashMap;
import java.util.Map;

public class LoadPoisson {
  private HashMap<String, Double> allPoissonMap = new HashMap<>();

  public synchronized void setAllPoisson(int avgLoad, HashMap<String, Integer> nodeOverLoadCount) {
    for (Map.Entry<String, Integer> entry : nodeOverLoadCount.entrySet()) {
      int x = getBinOneCount(entry.getValue());
      allPoissonMap.put(entry.getKey(), doPoisson(avgLoad, x));
    }
  }

  public double doPoisson(int lambda, int x) {
    double Probability = 0;
    double ans = 0;

    for (int i = 0; i <= x; i++) {
      double j = Math.pow(lambda, i);
      double e = Math.exp(-lambda);
      long h = factorial(i);
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
