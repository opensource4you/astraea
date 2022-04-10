package org.astraea.cost;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.partitioner.smoothPartitioner.SmoothWeightMetrics;

public class EntropyEmpowerment {

  public Map<String, Double> empowerment(Map<String, Map<Integer, Double>> bMetrics) {
    var metrics = new HashMap<String, Double>();
    metrics.put(
        SmoothWeightMetrics.BrokerMetrics.inputThroughput.metricName(),
        entropy(bMetrics.get(SmoothWeightMetrics.BrokerMetrics.inputThroughput.metricName())));
    metrics.put(
        SmoothWeightMetrics.BrokerMetrics.outputThroughput.metricName(),
        entropy(bMetrics.get(SmoothWeightMetrics.BrokerMetrics.outputThroughput.metricName())));
    metrics.put(
        SmoothWeightMetrics.BrokerMetrics.jvm.metricName(),
        entropy(bMetrics.get(SmoothWeightMetrics.BrokerMetrics.jvm.metricName())));
    metrics.put(
        SmoothWeightMetrics.BrokerMetrics.cpu.metricName(),
        entropy(bMetrics.get(SmoothWeightMetrics.BrokerMetrics.cpu.metricName())));
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
    //    System.out.println(positiveIndicator(metrics) + ":" + metrics);
    //    System.out.println(
    //        metricsWeight(positiveIndicator(metrics)).values().stream()
    //            .filter(weight -> !weight.equals(0.0))
    //            .collect(Collectors.toList()));
    return 1
        - metricsWeight(positiveIndicator(metrics)).values().stream()
                .filter(weight -> !weight.equals(0.0))
                .mapToDouble(weight -> weight * Math.log(weight))
                .sum()
            / (-Math.log(metrics.size()));
  }
}
