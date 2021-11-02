package org.astraea.partitioner.nodeLoadMetric;

import java.util.HashMap;
import java.util.Map;

public class LoadPoisson {
  private NodeLoadClient nodeLoadClient;

  public LoadPoisson(NodeLoadClient nodeLoadClient) {
    this.nodeLoadClient = nodeLoadClient;
  }

  public synchronized HashMap<String, Double> setAllPoisson() {
    HashMap<String, Double> poissonMap = new HashMap<>();
    int lambda = nodeLoadClient.getAvgLoadCount();
    for (Map.Entry<String, Integer> entry : nodeLoadClient.getAllOverLoadCount().entrySet()) {
      int x = nodeLoadClient.getBinOneCount(entry.getValue());
      poissonMap.put(entry.getKey(), doPoisson(lambda, x));
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
}
