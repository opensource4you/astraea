package org.astraea.cost;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.partitioner.smoothPartitioner.DynamicWeightsMetrics;

/**
 * Entropy weight method (EWM) is a commonly used weighting method that measures value dispersion in
 * decision-making. The greater the degree of dispersion, the greater the degree of differentiation,
 * and more information can be derived. Meanwhile, higher weight should be given to the index, and
 * vice versa.
 */
public class EntropyEmpowerment {

  public Map<String, Double> empowerment(Map<String, Map<Integer, Double>> bMetrics) {
    var metrics = new HashMap<String, Double>();
    metrics.put(
        DynamicWeightsMetrics.BrokerMetrics.inputThroughput.metricName(),
        entropy(bMetrics.get(DynamicWeightsMetrics.BrokerMetrics.inputThroughput.metricName())));
    metrics.put(
        DynamicWeightsMetrics.BrokerMetrics.outputThroughput.metricName(),
        entropy(bMetrics.get(DynamicWeightsMetrics.BrokerMetrics.outputThroughput.metricName())));
    metrics.put(
        DynamicWeightsMetrics.BrokerMetrics.jvm.metricName(),
        entropy(bMetrics.get(DynamicWeightsMetrics.BrokerMetrics.jvm.metricName())));
    metrics.put(
        DynamicWeightsMetrics.BrokerMetrics.cpu.metricName(),
        entropy(bMetrics.get(DynamicWeightsMetrics.BrokerMetrics.cpu.metricName())));
    var sum = metrics.values().stream().mapToDouble(i -> i).sum();

    return metrics.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue() / sum));
  }

  Map<Integer, Double> negativeIndicator(Map<Integer, Double> metrics) {
    Comparator<Double> comparator = Comparator.comparing(Double::doubleValue);
    var max = metrics.values().stream().max(comparator).orElse(0.0);
    var min = metrics.values().stream().min(comparator).orElse(0.0);
    return metrics.entrySet().stream()
        .collect(
            Collectors.toMap(Map.Entry::getKey, entry -> (max - entry.getValue()) / (max - min)));
  }

  Map<Integer, Double> positiveIndicator(Map<Integer, Double> metrics) {
    Comparator<Double> comparator = Comparator.comparing(Double::doubleValue);
    var max = metrics.values().stream().max(comparator).orElse(0.0) + 0.01;
    var min = metrics.values().stream().min(comparator).orElse(0.0);
    return metrics.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> (entry.getValue() + 0.01 - min) / (max - min)));
  }

  Map<Integer, Double> metricsWeight(Map<Integer, Double> indicator) {
    var sum = indicator.values().stream().mapToDouble(i -> i).sum();
    return indicator.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue() / sum));
  }

  double entropy(Map<Integer, Double> metrics) {
    return 1
        - metricsWeight(positiveIndicator(metrics)).values().stream()
                .filter(weight -> !weight.equals(0.0))
                .mapToDouble(weight -> weight * Math.log(weight))
                .sum()
            / (-Math.log(metrics.size()));
  }
}
