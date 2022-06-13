package org.astraea.cost.broker;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.cost.BrokerCost;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.CostFunction;
import org.astraea.cost.HasBrokerCost;
import org.astraea.metrics.collector.Fetcher;

/**
 * The result is computed by four cost function.There are "BrokerInputCost", "BrokerOutputCost",
 * "CpuCost" and "MemoryCost". We used a combination of AHP(Analytic Hierarchy Process) and entropy
 * weighting to integrate these data into a score that is representative of Broker's load profile.
 * Because AHP is a subjective evaluation method and entropy method is an objective evaluation
 * method, so I named it NeutralIntegratedCost.
 */
public class NeutralIntegratedCost implements HasBrokerCost {
  private final List<HasBrokerCost> metricsCost =
      List.of(new BrokerInputCost(), new BrokerOutputCost(), new CpuCost(), new MemoryCost());
  private final Map<Integer, BrokerMetrics> brokersMetric = new HashMap<>();
  private final EntropyEmpowerment entropyEmpowerment = new EntropyEmpowerment();
  private final AHPEmpowerment ahpEmpowerment = new AHPEmpowerment();

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    var costMetrics =
        clusterInfo.beans().broker().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> 0.0));
    costMetrics.forEach(
        (key, value) -> {
          if (!brokersMetric.containsKey(key)) {
            brokersMetric.put(key, new BrokerMetrics());
          }
        });

    metricsCost.forEach(
        hasBrokerCost -> {
          if (hasBrokerCost instanceof BrokerInputCost) {
            hasBrokerCost
                .brokerCost(clusterInfo)
                .value()
                .forEach((brokerID, value) -> brokersMetric.get(brokerID).inputScore = value);
          } else if (hasBrokerCost instanceof BrokerOutputCost) {
            hasBrokerCost
                .brokerCost(clusterInfo)
                .value()
                .forEach((brokerID, value) -> brokersMetric.get(brokerID).outputScore = value);
          } else if (hasBrokerCost instanceof CpuCost) {
            hasBrokerCost
                .brokerCost(clusterInfo)
                .value()
                .forEach((brokerID, value) -> brokersMetric.get(brokerID).cpuScore = value);
          } else if (hasBrokerCost instanceof MemoryCost) {
            hasBrokerCost
                .brokerCost(clusterInfo)
                .value()
                .forEach((brokerID, value) -> brokersMetric.get(brokerID).memoryScore = value);
          }
        });

    var integratedEmpowerment =
        entropyEmpowerment.empowerment(brokersMetric).entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue() * 0.5
                            + ahpEmpowerment.empowerment().get(entry.getKey()) * 0.5));

    return () ->
        brokersMetric.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().inputScore
                                * integratedEmpowerment.get(Metrics.inputThroughput.metricName)
                            + entry.getValue().outputScore
                                * integratedEmpowerment.get(Metrics.outputThroughput.metricName)
                            + entry.getValue().cpuScore
                                * integratedEmpowerment.get(Metrics.cpu.metricName)
                            + entry.getValue().memoryScore
                                * integratedEmpowerment.get(Metrics.memory.metricName)));
  }

  @Override
  public Fetcher fetcher() {
    return Fetcher.of(metricsCost.stream().map(CostFunction::fetcher).collect(Collectors.toList()));
  }

  static class BrokerMetrics {
    double inputScore = 0.0;
    double outputScore = 0.0;
    double cpuScore = 0.0;
    double memoryScore = 0.0;

    BrokerMetrics() {}
  }

  /**
   * Entropy weight method (EWM) is a commonly used weighting method that measures value dispersion
   * in decision-making. The greater the degree of dispersion, the greater the degree of
   * differentiation, and more information can be derived. Meanwhile, higher weight should be given
   * to the index, and vice versa.
   */
  static class EntropyEmpowerment {
    public Map<String, Double> empowerment(Map<Integer, BrokerMetrics> brokerMetrics) {
      var metrics = new HashMap<String, Double>();
      metrics.put(
          Metrics.inputThroughput.metricName,
          entropy(
              brokerMetrics.entrySet().stream()
                  .collect(
                      Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().inputScore))));
      metrics.put(
          Metrics.outputThroughput.metricName,
          entropy(
              brokerMetrics.entrySet().stream()
                  .collect(
                      Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().outputScore))));
      metrics.put(
          Metrics.memory.metricName,
          entropy(
              brokerMetrics.entrySet().stream()
                  .collect(
                      Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().memoryScore))));
      metrics.put(
          Metrics.cpu.metricName,
          entropy(
              brokerMetrics.entrySet().stream()
                  .collect(
                      Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().cpuScore))));
      var sum = metrics.values().stream().mapToDouble(i -> i).sum();

      return metrics.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue() / sum));
    }

    /**
     * To the cost indexes
     *
     * @param metrics JMX metrics for each broker.
     * @return Standardization of result
     */
    Map<Integer, Double> negativeIndicator(Map<Integer, Double> metrics) {
      Comparator<Double> comparator = Comparator.comparing(Double::doubleValue);
      var max = metrics.values().stream().max(comparator).orElse(0.0);
      var min = metrics.values().stream().min(comparator).orElse(0.0);
      return metrics.entrySet().stream()
          .collect(
              Collectors.toMap(Map.Entry::getKey, entry -> (max - entry.getValue()) / (max - min)));
    }

    /**
     * To the benefit indexes.
     *
     * @param metrics JMX metrics for each broker.
     * @return Standardization of result
     */
    Map<Integer, Double> positiveIndicator(Map<Integer, Double> metrics) {
      Comparator<Double> comparator = Comparator.comparing(Double::doubleValue);
      var max = metrics.values().stream().max(comparator).orElse(0.0) + 0.01;
      double min = metrics.values().stream().min(comparator).orElse(0.0);
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

  /**
   * AHP is a structured technique for organizing and solving complex decision-making problems based
   * on mathematics and psychology. AHP provides a comprehensive and logical framework to quantify
   * each structural decision-making element within a hierarchical structure. The AHP begins with
   * choosing the decision criteria.
   *
   * <p>This is the pairwise comparison matrix currently being used by Astraea Partitioner.
   *
   * <p>||==============||===============||================||==========||=============||========||
   * || Metrics --------||InputThroughput||OutputThroughput|| CpuUsage || MemoryUsage || Weight ||
   * || InputThroughput-||------ 1 ------||------ 4 -------||--- 6 ----||----- 6 -----|| 0.6003 ||
   * || outputThroughput||----- 1/4 -----||------ 1 -------||--- 4 ----||----- 4 -----|| 0.2434 ||
   * || CpuUsage--------||----- 1/6 -----||----- 1/4 ------||--- 1 ----||---- 1/2 ----|| 0.0914 ||
   * || MemoryUsage-----||----- 1/6 -----||----- 1/4 ------||--- 2 ----||----- 1 -----|| 0.0649 ||
   * ||=================||===============||================||==========||=============||========||
   */
  private static class AHPEmpowerment {
    private final Map<String, Double> balanceThroughput =
        Map.of(
            Metrics.inputThroughput.metricName(),
            0.6003,
            Metrics.outputThroughput.metricName(),
            0.2434,
            Metrics.cpu.metricName(),
            0.0649,
            Metrics.memory.metricName(),
            0.0914);

    public Map<String, Double> empowerment() {
      return balanceThroughput;
    }
  }

  private enum Metrics {
    inputThroughput("inputThroughput"),
    outputThroughput("outputThroughput"),
    memory("memory"),
    cpu("cpu");

    private final String metricName;

    Metrics(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }
  }

  EntropyEmpowerment entropyEmpowerment() {
    return entropyEmpowerment;
  }
}
