package org.astraea.partitioner.nodeLoadMetric;

import java.util.HashMap;
import java.util.Map;

/** do poisson for node's load situation */
public class LoadPoisson {

  public HashMap<Integer, Double> allPoisson(Map<Integer, Integer> overLoadCount) {
    var poissonMap = new HashMap<Integer, Double>();
    var lambda = avgLoadCount(overLoadCount);
    overLoadCount.forEach((nodeID, count) -> poissonMap.put(nodeID, doPoisson(lambda, count)));
    return poissonMap;
  }

  // visible for test
  double doPoisson(int lambda, int x) {
    var Probability = 0.0;
    var ans = 0.0;
    var i = 0;
    while (i <= x) {
      var j = Math.pow(lambda, i);
      var e = Math.exp(-lambda);
      var h = factorial(i);
      Probability = (j * e) / h;
      ans += Probability;
      i++;
    }
    return ans;
  }

  // visible for test
  long factorial(long number) {
    if (number <= 1) return 1;
    else return number * factorial(number - 1);
  }

  private int avgLoadCount(Map<Integer, Integer> overLoadCount) {
    var avgLoadCount =
        overLoadCount.values().stream().mapToDouble(Integer::doubleValue).average().orElse(0);
    return (int) avgLoadCount;
  }
}
