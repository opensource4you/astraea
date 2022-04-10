package org.astraea.cost;

import java.util.Map;
import org.astraea.partitioner.smoothPartitioner.SmoothWeightMetrics;

// TODO
public class ANPEmpowerment {
  private static Map<String, Double> balanceThroughput =
      Map.of(
          SmoothWeightMetrics.BrokerMetrics.inputThroughput.metricName(),
          0.7,
          SmoothWeightMetrics.BrokerMetrics.outputThroughput.metricName(),
          0.2,
          SmoothWeightMetrics.BrokerMetrics.cpu.metricName(),
          0.1,
          SmoothWeightMetrics.BrokerMetrics.jvm.metricName(),
          0.1);

  public Map<String, Double> empowerment(SmoothWeightMetrics smoothWeightMetrics) {
    return balanceThroughput;
  }
}
