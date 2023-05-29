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
package org.astraea.common.cost;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.EnumInfo;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.collector.MetricSensor;

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
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    // The local MBean will affect the partitioner's judgment, so filtering out the local bean.
    var clusterBeanWithoutLocal = ClusterBean.masked(clusterBean, node -> node != -1);
    clusterBeanWithoutLocal.all().keySet().stream()
        .filter(broker -> !brokersMetric.containsKey(broker))
        .forEach(broker -> brokersMetric.put(broker, new BrokerMetrics()));

    metricsCost.forEach(
        hasBrokerCost -> setBrokerMetrics(hasBrokerCost, clusterInfo, clusterBeanWithoutLocal));

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
  // TODO Refactor
  void setBrokerMetrics(
      HasBrokerCost hasBrokerCost, ClusterInfo clusterInfo, ClusterBean clusterBean) {
    if (hasBrokerCost instanceof BrokerInputCost) {
      var inputBrokerCost = hasBrokerCost.brokerCost(clusterInfo, clusterBean);
      inputBrokerCost
          .value()
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).inputScore = value);
      Normalizer.TScore()
          .normalize(inputBrokerCost.value())
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).inputTScore = value);
    } else if (hasBrokerCost instanceof BrokerOutputCost) {
      var outPutBrokerCost = hasBrokerCost.brokerCost(clusterInfo, clusterBean);
      outPutBrokerCost
          .value()
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).outputScore = value);
      Normalizer.TScore()
          .normalize(outPutBrokerCost.value())
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).outputTScore = value);
    } else if (hasBrokerCost instanceof CpuCost) {
      var cpubrokercost = hasBrokerCost.brokerCost(clusterInfo, clusterBean);
      cpubrokercost
          .value()
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).cpuScore = value);
      Normalizer.TScore()
          .normalize(cpubrokercost.value())
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).cpuTScore = value);
    } else if (hasBrokerCost instanceof MemoryCost) {
      var memoryBrokerCost = hasBrokerCost.brokerCost(clusterInfo, clusterBean);
      memoryBrokerCost
          .value()
          .forEach((brokerID, value) -> brokersMetric.get(brokerID).memoryScore = value);
      Normalizer.TScore()
          .normalize(memoryBrokerCost.value())
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
                    m ->
                        switch (m) {
                          case inputThroughput -> brokerMetrics.values().stream()
                              .map(metrics -> metrics.inputScore)
                              .toList();
                          case outputThroughput -> brokerMetrics.values().stream()
                              .map(metrics -> metrics.outputScore)
                              .toList();
                          case memory -> brokerMetrics.values().stream()
                              .map(metrics -> metrics.memoryScore)
                              .toList();
                          case cpu -> brokerMetrics.values().stream()
                              .map(metrics -> metrics.cpuScore)
                              .toList();
                        }));
    return weightProvider.weight(values);
  }

  @Override
  public MetricSensor metricSensor() {
    return MetricSensor.of(metricsCost.stream().map(CostFunction::metricSensor).toList());
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
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

  private enum Metrics implements EnumInfo {
    inputThroughput("inputThroughput"),
    outputThroughput("outputThroughput"),
    memory("memory"),
    cpu("cpu");

    public static Metrics ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Metrics.class, alias);
    }

    private final String metricName;

    Metrics(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    @Override
    public String alias() {
      return metricName();
    }

    @Override
    public String toString() {
      return alias();
    }
  }
}
