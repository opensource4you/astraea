package org.astraea.partitioner;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/** do poisson for node's load situation */
public class PartitionerUtils {
  private PartitionerUtils() {}

  public static Map<Integer, Double> allPoisson(Map<Integer, Integer> overLoadCount) {
    var poissonMap = new HashMap<Integer, Double>();
    var lambda = avgLoadCount(overLoadCount);
    overLoadCount.forEach((nodeID, count) -> poissonMap.put(nodeID, doPoisson(lambda, count)));
    return poissonMap;
  }

  public static int weightPoisson(Double value, Double thoughPutAbility) {
    if (value < 0.8) return (int) (Math.round((1 - value) * 20 * thoughPutAbility));
    else return (int) Math.pow(Math.round((1 - value) * 20 * thoughPutAbility), 2) / 10;
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
    return (int) Math.round(avgLoadCount);
  }

  public static Properties partitionerConfig(Map<String, ?> configs) {
    var properties = new Properties();
    try {
      properties.load(new FileInputStream((String) configs.get("partitioner.config")));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return properties;
  }

  /**
   * This method helps extract the config of form "broker.<brokerID>.jmx.port"="<port>" to a map.
   * For example, the configuration {"broker.1001.jmx.port"="8000", "broker.1002.jmx.port"="8001"}
   * will be parsed to map {(1001,8000), (1002,8001)}.
   *
   * @param config contains the config of broker id and its jmx port.
   * @return pairs of broker id and its jmx port
   */
  public static Map<Integer, Integer> parseIdJMXPort(Configuration config) {
    return config.entrySet().stream()
        .filter(e -> e.getKey().startsWith("broker."))
        .filter(e -> e.getKey().endsWith("jmx.port"))
        .map(
            e ->
                Map.entry(
                    e.getKey().replaceAll("broker[.]", "").replaceAll("[.]jmx[.]port", ""),
                    e.getValue()))
        .map(e -> Map.entry(Integer.parseInt(e.getKey()), Integer.parseInt(e.getValue())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
