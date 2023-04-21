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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.MetricsTestUtils;
import org.astraea.common.metrics.platform.OperatingSystemInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CpuCostTest {

  @Test
  void testCost() {
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1,
                List.of(mockResult(0.5, 1), mockResult(0.3, 0)),
                2,
                List.of(mockResult(0.6, 0)),
                3,
                List.of(mockResult(0.7, 0)),
                4,
                List.of()));
    var cpuCost = new CpuCost();
    var result = cpuCost.brokerCost(ClusterInfo.empty(), clusterBean);
    Assertions.assertEquals(0.5, result.value().get(1));
    Assertions.assertEquals(0.6, result.value().get(2));
    Assertions.assertEquals(0.7, result.value().get(3));
    Assertions.assertEquals(0D, result.value().get(4));
  }

  @Test
  void testSensor() {
    var f = new CpuCost();
    var clusterBean =
        MetricsTestUtils.clusterBean(Map.of(0, MBeanClient.local()), f.metricSensor().get());
    Assertions.assertFalse(
        clusterBean.brokerMetrics(0, OperatingSystemInfo.class).findAny().isEmpty());
    Assertions.assertTrue(
        clusterBean.brokerMetrics(0, OperatingSystemInfo.class).allMatch(Objects::nonNull));

    // Test if we can get the value between 0 ~ 1
    Assertions.assertTrue(
        clusterBean
            .brokerMetrics(0, OperatingSystemInfo.class)
            .allMatch(
                OSInfo -> (OSInfo.systemCpuLoad() <= 1.0) && (OSInfo.systemCpuLoad() >= 0.0)));
  }

  private static OperatingSystemInfo mockResult(double usage, long createdTimestamp) {
    var cpu = Mockito.mock(OperatingSystemInfo.class);
    Mockito.when(cpu.systemCpuLoad()).thenReturn(usage);
    Mockito.when(cpu.createdTimestamp()).thenReturn(createdTimestamp);
    return cpu;
  }
}
