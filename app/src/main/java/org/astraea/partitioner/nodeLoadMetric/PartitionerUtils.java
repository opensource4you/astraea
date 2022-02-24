package org.astraea.partitioner.nodeLoadMetric;

import java.util.HashMap;
import java.util.Map;

/** do poisson for node's load situation */
public class PartitionerUtils {
  private PartitionerUtils() {}

  public static HashMap<Integer, Double> allPoisson(Map<Integer, Integer> overLoadCount) {
    var poissonMap = new HashMap<Integer, Double>();
    var lambda = avgLoadCount(overLoadCount);
    overLoadCount.forEach((nodeID, count) -> poissonMap.put(nodeID, doPoisson(lambda, count)));
    return poissonMap;
  }

  static double doPoisson(int lambda, int x) {
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

  static long factorial(long number) {
    if (number <= 1) return 1;
    else return number * factorial(number - 1);
  }

  private static int avgLoadCount(Map<Integer, Integer> overLoadCount) {
    var avgLoadCount =
        overLoadCount.values().stream().mapToDouble(Integer::doubleValue).average().orElse(0);
    return (int) avgLoadCount;
  }

  /**
   * A cheap way to deterministically convert a number to a positive value. When the input is
   * positive, the original value is returned. When the input number is negative, the returned
   * positive value is the original value bit AND against 0x7fffffff which is not its absolute
   * value.
   *
   * <p>Note: changing this method in the future will possibly cause partition selection not to be
   * compatible with the existing messages already placed on a partition since it is used in
   * producer's {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner}
   *
   * @param number a given number
   * @return a positive number.
   */
  public static int toPositive(int number) {
    return number & 0x7fffffff;
  }
}
