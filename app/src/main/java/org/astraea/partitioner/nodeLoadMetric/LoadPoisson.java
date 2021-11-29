package org.astraea.partitioner.nodeLoadMetric;

import java.util.HashMap;
import java.util.Map;

public class LoadPoisson {
  private Map<Integer, Double> allPoisson = new HashMap<>();

  public synchronized void allNodesPoisson(Map<Integer, Integer> nodeOverLoadCount) {
    int avgLoad =
        nodeOverLoadCount.values().stream().reduce(Integer::sum).get() / nodeOverLoadCount.size();
    for (Map.Entry<Integer, Integer> entry : nodeOverLoadCount.entrySet()) {
      allPoisson.put(entry.getKey(), doPoisson(avgLoad, entry.getValue()));
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

  public Map<Integer, Double> getAllPoisson() {
    return allPoisson;
  }
}
