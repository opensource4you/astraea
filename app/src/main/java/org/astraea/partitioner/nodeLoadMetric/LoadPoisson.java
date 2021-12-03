package org.astraea.partitioner.nodeLoadMetric;

import java.util.HashMap;
import java.util.Map;

/** do poisson for node's load situation */
public class LoadPoisson {

  public synchronized HashMap<Integer, Double> allPoisson(Map<Integer, Integer> overLoadCount) {
    var poissonMap = new HashMap<Integer, Double>();
    var lambda = avgLoadCount(overLoadCount);
    for (Map.Entry<Integer, Integer> entry : overLoadCount.entrySet()) {
      poissonMap.put(entry.getKey(), doPoisson(lambda, entry.getValue()));
    }
    return poissonMap;
  }

  public double doPoisson(int lambda, int x) {
    var Probability = 0.0;
    var ans = 0.0;

    for (var i = 0; i <= x; i++) {
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

  private int avgLoadCount(Map<Integer, Integer> overLoadCount) {
    var avgLoadCount =
        overLoadCount.values().stream().mapToDouble(Integer::doubleValue).average().getAsDouble();
    return (int) avgLoadCount;
  }
}
