package org.astraea.cost.DynamicWeightsUtils;

import java.util.Map;
import org.astraea.partitioner.smoothPartitioner.DynamicWeightsMetrics;

/**
 * AHP is a structured technique for organizing and solving complex decision-making problems based
 * on mathematics and psychology. AHP provides a comprehensive and logical framework to quantify
 * each structural decision-making element within a hierarchical structure. The AHP begins with
 * choosing the decision criteria.
 */
// TODO
public class AHPEmpowerment {
  private static Map<String, Double> balanceThroughput =
      Map.of(
          DynamicWeightsMetrics.BrokerMetrics.inputThroughput.metricName(),
          0.6,
          DynamicWeightsMetrics.BrokerMetrics.outputThroughput.metricName(),
          0.2,
          DynamicWeightsMetrics.BrokerMetrics.cpu.metricName(),
          0.1,
          DynamicWeightsMetrics.BrokerMetrics.jvm.metricName(),
          0.1);

  public Map<String, Double> empowerment(DynamicWeightsMetrics dynamicWeightsMetrics) {
    return balanceThroughput;
  }
}
