package org.astraea.partitioner.nodeLoadMetric;

import java.util.HashMap;
import java.util.Map;

public class LoadPoisson {

  public synchronized HashMap<Integer, Double> setAllPoisson(Map<Integer, Integer> overLoadCount) {
    HashMap<Integer, Double> poissonMap = new HashMap<>();
    var lambda = getAvgLoadCount(overLoadCount);
    for (Map.Entry<Integer, Integer> entry : overLoadCount.entrySet()) {
      poissonMap.put(entry.getKey(), doPoisson(lambda, entry.getValue()));
    }
    return poissonMap;
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

  private int getAvgLoadCount(Map<Integer, Integer> overLoadCount) {
    var avgLoadCount =
        overLoadCount.values().stream().mapToDouble(Integer::doubleValue).average().getAsDouble();
    return (int) avgLoadCount;
  }
}
