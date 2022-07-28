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

import java.util.List;
import java.util.Map;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.metrics.collector.BeanCollector;
import org.astraea.app.metrics.collector.Receiver;
import org.astraea.app.metrics.platform.OperatingSystemInfo;
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
                List.of(mockResult(0.5)),
                2,
                List.of(mockResult(0.6)),
                3,
                List.of(mockResult(0.7)),
                4,
                List.of()));
    var cpuCost = new CpuCost();
    var result = cpuCost.brokerCost(Mockito.mock(ClusterInfo.class), clusterBean);
    Assertions.assertEquals(0.5, result.value().get(1));
    Assertions.assertEquals(0.6, result.value().get(2));
    Assertions.assertEquals(0.7, result.value().get(3));
    Assertions.assertEquals(0D, result.value().get(4));
  }

  @Test
  void testFetcher() {
    try (Receiver receiver =
        BeanCollector.builder()
            .build()
            .register()
            .local()
            .fetcher(new CpuCost().fetcher().get())
            .build()) {
      Assertions.assertFalse(receiver.current().isEmpty());
      Assertions.assertTrue(
          receiver.current().stream().allMatch(o -> o instanceof OperatingSystemInfo));

      // Test if we can get the value between 0 ~ 1
      Assertions.assertTrue(
          receiver.current().stream()
              .map(o -> (OperatingSystemInfo) o)
              .allMatch(
                  OSInfo -> (OSInfo.systemCpuLoad() <= 1.0) && (OSInfo.systemCpuLoad() >= 0.0)));
    }
  }

  private static OperatingSystemInfo mockResult(double usage) {
    var cpu = Mockito.mock(OperatingSystemInfo.class);
    Mockito.when(cpu.systemCpuLoad()).thenReturn(usage);
    return cpu;
  }
}
