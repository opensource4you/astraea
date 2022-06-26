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
package org.astraea.app.cost.broker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.astraea.app.cost.BrokerCost;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.HasBrokerCost;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class NeutralIntegratedCostTest {
  @Test
  void testSetBrokerMetrics() throws NoSuchFieldException, IllegalAccessException {
    var neutralIntegratedCost = new NeutralIntegratedCost();
    var brokerMetricsField = neutralIntegratedCost.getClass().getDeclaredField("brokersMetric");
    var clusterInfo = Mockito.mock(ClusterInfo.class);
    brokerMetricsField.setAccessible(true);
    brokerMetricsField.set(
        neutralIntegratedCost,
        Map.of(
            0,
            new NeutralIntegratedCost.BrokerMetrics(),
            1,
            new NeutralIntegratedCost.BrokerMetrics(),
            2,
            new NeutralIntegratedCost.BrokerMetrics()));

    HasBrokerCost input = Mockito.mock(BrokerInputCost.class);
    HasBrokerCost output = Mockito.mock(BrokerOutputCost.class);
    HasBrokerCost cpu = Mockito.mock(CpuCost.class);
    HasBrokerCost memory = Mockito.mock(MemoryCost.class);

    var inputBrokerCost = getBrokerCost(new HashMap<>(Map.of(0, 50.0, 1, 100.0, 2, 150.0)));
    var outputBrokerCost = getBrokerCost(new HashMap<>(Map.of(0, 500.0, 1, 1000.0, 2, 1500.0)));
    var cpuBrokerCost = getBrokerCost(new HashMap<>(Map.of(0, 0.05, 1, 0.1, 2, 0.15)));
    var memoryBrokerCost = getBrokerCost(new HashMap<>(Map.of(0, 0.1, 1, 0.3, 2, 0.5)));
    Mockito.when(input.brokerCost(clusterInfo)).thenReturn(inputBrokerCost);
    Mockito.when(output.brokerCost(clusterInfo)).thenReturn(outputBrokerCost);
    Mockito.when(memory.brokerCost(clusterInfo)).thenReturn(memoryBrokerCost);
    Mockito.when(cpu.brokerCost(clusterInfo)).thenReturn(cpuBrokerCost);
    List<HasBrokerCost> metricsCost = List.of(input, output, cpu, memory);
    metricsCost.forEach(
        hasBrokerCost -> neutralIntegratedCost.setBrokerMetrics(hasBrokerCost, clusterInfo));

    var metrics = (Map) brokerMetricsField.get(neutralIntegratedCost);
    var inputField = NeutralIntegratedCost.BrokerMetrics.class.getDeclaredField("inputScore");
    var outputField = NeutralIntegratedCost.BrokerMetrics.class.getDeclaredField("outputScore");
    var memoryTField = NeutralIntegratedCost.BrokerMetrics.class.getDeclaredField("memoryTScore");
    var cpuTField = NeutralIntegratedCost.BrokerMetrics.class.getDeclaredField("cpuTScore");

    Assertions.assertEquals(inputField.get(metrics.get(0)), 50.0);
    Assertions.assertEquals(inputField.get(metrics.get(1)), 100.0);
    Assertions.assertEquals(inputField.get(metrics.get(2)), 150.0);
    Assertions.assertEquals(outputField.get(metrics.get(0)), 500.0);
    Assertions.assertEquals(outputField.get(metrics.get(1)), 1000.0);
    Assertions.assertEquals(outputField.get(metrics.get(2)), 1500.0);
    cpuTField.setAccessible(true);
    memoryTField.setAccessible(true);

    Assertions.assertEquals(cpuTField.get(metrics.get(0)), 0.38);
    Assertions.assertEquals(cpuTField.get(metrics.get(1)), 0.5);
    Assertions.assertEquals(cpuTField.get(metrics.get(2)), 0.62);
    Assertions.assertEquals(memoryTField.get(metrics.get(0)), 0.38);
    Assertions.assertEquals(memoryTField.get(metrics.get(1)), 0.5);
    Assertions.assertEquals(memoryTField.get(metrics.get(2)), 0.62);
  }

  BrokerCost getBrokerCost(Map<Integer, Double> map) {
    return () -> map;
  }
}
