/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.cost;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.metrics.collector.Fetcher;

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
  // Visible for test
  Map<Integer, BrokerMetrics> brokersMetric = new HashMap<>();
  private final AHPEmpowerment ahpEmpowerment = new AHPEmpowerment();
  private final WeightProvider weightProvider = WeightProvider.entropy(Normalizer.minMax(true));

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    var costMetrics =
        clusterInfo.clusterBean().all().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> 0.0));
    costMetrics.forEach(
        (key, value) -> {
          if (!brokersMetric.containsKey(key)) {
            brokersMetric.put(key, new BrokerMetrics());
          }
        });

    metricsCost.forEach(hasBrokerCost -> setBrokerMetrics(hasBrokerCost, clusterInfo));

    var entropyEmpowerment = weight(weightProvider, brokersMetric);
    var entropyEmpowermentSum =
        entropyEmpowerment.entrySet().stream()
            .mapToDouble(
                entry -> entry.getValue() * ahpEmpowerment.empowerment().get(entry.getKey()))
            .sum();
    // The weight of each metric is obtained by combining Entropy and AHP.
    var integratedEmpowerment =
        entropyEmpowerment.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue()
                            * ahpEmpowerment.empowerment().get(entry.getKey())
                            / entropyEmpowermentSum));

    var integratedScore =
        brokersMetric.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().inputTScore
                                * integratedEmpowerment.get(Metrics.inputThroughput.metricName)
                            + entry.getValue().outputTScore
                                * integratedEmpowerment.get(Metrics.outputThroughput.metricName)
                            + entry.getValue().cpuTScore
                                * integratedEmpowerment.get(Metrics.cpu.metricName)
                            + entry.getValue().memoryTScore
                                * integratedEmpowerment.get(Metrics.memory.metricName)));

    return () -> integratedScore;
  }

  // Save the original value of each metric and the value of the metric after the TScore
  // calculation.
  void setBrokerMetrics(HasBrokerCost hasBrokerCost, ClusterInfo clusterInfo) {
    if (hasBrokerCost instanceof BrokerInputCost) {
      var inputBrokerCost = hasBrokerCost.brokerCost(clusterInfo);
      inputBrokerCost
          .value()
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).inputScore = value);
      inputBrokerCost
          .normalize(Normalizer.TScore())
          .value()
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).inputTScore = value);
    } else if (hasBrokerCost instanceof BrokerOutputCost) {
      var outPutBrokerCost = hasBrokerCost.brokerCost(clusterInfo);
      outPutBrokerCost
          .value()
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).outputScore = value);
      outPutBrokerCost
          .normalize(Normalizer.TScore())
          .value()
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).outputTScore = value);
    } else if (hasBrokerCost instanceof CpuCost) {
      var CPUBrokerCost = hasBrokerCost.brokerCost(clusterInfo);
      CPUBrokerCost.value()
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).cpuScore = value);
      CPUBrokerCost.normalize(Normalizer.TScore())
          .value()
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).cpuTScore = value);
    } else if (hasBrokerCost instanceof MemoryCost) {
      var MemoryBrokerCost = hasBrokerCost.brokerCost(clusterInfo);
      MemoryBrokerCost.value()
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).memoryScore = value);
      MemoryBrokerCost.normalize(Normalizer.TScore())
          .value()
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).memoryTScore = value);
    }
  }

  static Map<String, Double> weight(
      WeightProvider weightProvider, Map<Integer, BrokerMetrics> brokerMetrics) {

    var values =
        Arrays.stream(Metrics.values())
            .collect(
                Collectors.toMap(
                    m -> m.metricName,
                    m -> {
                      switch (m) {
                        case inputThroughput:
                          return brokerMetrics.values().stream()
                              .map(metrics -> metrics.inputScore)
                              .collect(Collectors.toUnmodifiableList());
                        case outputThroughput:
                          return brokerMetrics.values().stream()
                              .map(metrics -> metrics.outputScore)
                              .collect(Collectors.toUnmodifiableList());
                        case memory:
                          return brokerMetrics.values().stream()
                              .map(metrics -> metrics.memoryScore)
                              .collect(Collectors.toUnmodifiableList());
                        case cpu:
                          return brokerMetrics.values().stream()
                              .map(metrics -> metrics.cpuScore)
                              .collect(Collectors.toUnmodifiableList());
                        default:
                          return List.<Double>of();
                      }
                    }));
    return weightProvider.weight(values);
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
    double inputTScore = 0.0;
    double outputTScore = 0.0;
    double cpuTScore = 0.0;
    double memoryTScore = 0.0;

    BrokerMetrics() {}
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
}
