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

import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.Map;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.metrics.collector.BeanCollector;
import org.astraea.app.metrics.collector.Receiver;
import org.astraea.app.metrics.platform.HasJvmMemory;
import org.astraea.app.metrics.platform.JvmMemory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MemoryCostTest {

  @Test
  void testCost() {
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1,
                List.of(mockResult(30L, 100L, 0), mockResult(40L, 100L, 10)),
                2,
                List.of(mockResult(50L, 100L, 0)),
                3,
                List.of()));
    var memoryCost = new MemoryCost();
    var scores = memoryCost.brokerCost(ClusterInfo.empty(), clusterBean);
    Assertions.assertEquals(0.4, scores.value().get(1));
    Assertions.assertEquals(0.5, scores.value().get(2));
    Assertions.assertEquals(0, scores.value().get(3));
  }

  @Test
  void testFetcher() {
    try (Receiver receiver =
        BeanCollector.builder()
            .build()
            .register()
            .local()
            .fetcher(new MemoryCost().fetcher().get())
            .build()) {
      Assertions.assertFalse(receiver.current().isEmpty());
      Assertions.assertTrue(receiver.current().stream().allMatch(o -> o instanceof JvmMemory));

      // Test if we can get "used memory" and "max memory".
      Assertions.assertTrue(
          receiver.current().stream()
              .map(o -> (JvmMemory) o)
              .allMatch(mem -> mem.heapMemoryUsage().getUsed() <= mem.heapMemoryUsage().getMax()));
    }
  }

  private static HasJvmMemory mockResult(long used, long max, long createdTimestamp) {
    var jvmMemory = Mockito.mock(HasJvmMemory.class);
    var memoryUsage = Mockito.mock(MemoryUsage.class);
    Mockito.when(jvmMemory.heapMemoryUsage()).thenReturn(memoryUsage);
    Mockito.when(jvmMemory.createdTimestamp()).thenReturn(createdTimestamp);
    Mockito.when(memoryUsage.getUsed()).thenReturn(used);
    Mockito.when(memoryUsage.getMax()).thenReturn(max);
    return jvmMemory;
  }
}
