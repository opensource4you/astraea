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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.collector.BeanCollector;
import org.astraea.app.metrics.collector.Receiver;
import org.astraea.app.metrics.platform.OperatingSystemInfo;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CpuCostTest extends RequireBrokerCluster {
  @Test
  void testCost() throws InterruptedException {
    var cpuUsage1 = mockResult(0.5);
    var cpuUsage2 = mockResult(0.81);
    var cpuUsage3 = mockResult(0.62);

    Collection<HasBeanObject> broker1 = List.of(cpuUsage1);
    Collection<HasBeanObject> broker2 = List.of(cpuUsage2);
    Collection<HasBeanObject> broker3 = List.of(cpuUsage3);
    ClusterInfo clusterInfo =
        new FakeClusterInfo() {
          @Override
          public ClusterBean clusterBean() {
            return ClusterBean.of(Map.of(1, broker1, 2, broker2, 3, broker3));
          }

          @Override
          public Set<String> topics() {
            return Set.of("t");
          }

          @Override
          public List<ReplicaInfo> availableReplicas(String topic) {
            return List.of(
                ReplicaInfo.of("t", 0, NodeInfo.of(1, "host1", 9092), true, true, false),
                ReplicaInfo.of("t", 0, NodeInfo.of(2, "host2", 9092), false, true, false),
                ReplicaInfo.of("t", 0, NodeInfo.of(3, "host3", 9092), false, true, false));
          }
        };

    var cpuCost = new CpuCost();
    var scores = cpuCost.brokerCost(clusterInfo).normalize(Normalizer.TScore()).value();
    Assertions.assertEquals(0.39, scores.get(1));
    Assertions.assertEquals(0.63, scores.get(2));
    Assertions.assertEquals(0.48, scores.get(3));

    Thread.sleep(1000);
    cpuUsage1 = mockResult(0.8);
    cpuUsage2 = mockResult(0.5);
    cpuUsage3 = mockResult(0.3);

    Collection<HasBeanObject> broker12 = List.of(cpuUsage1);
    Collection<HasBeanObject> broker22 = List.of(cpuUsage2);
    Collection<HasBeanObject> broker32 = List.of(cpuUsage3);
    ClusterInfo clusterInfo2 =
        new FakeClusterInfo() {
          @Override
          public ClusterBean clusterBean() {
            return ClusterBean.of(Map.of(1, broker12, 2, broker22, 3, broker32));
          }

          @Override
          public Set<String> topics() {
            return Set.of("t");
          }

          @Override
          public List<ReplicaInfo> availableReplicas(String topic) {
            return List.of(
                ReplicaInfo.of("t", 0, NodeInfo.of(1, "host1", 9092), true, true, false),
                ReplicaInfo.of("t", 0, NodeInfo.of(2, "host2", 9092), false, true, false),
                ReplicaInfo.of("t", 0, NodeInfo.of(3, "host3", 9092), false, true, false));
          }
        };
    scores = cpuCost.brokerCost(clusterInfo2).normalize(Normalizer.TScore()).value();
    Assertions.assertEquals(0.52, scores.get(1));
    Assertions.assertEquals(0.61, scores.get(2));
    Assertions.assertEquals(0.37, scores.get(3));
  }

  @Test
  void testFetcher() {
    try (Receiver receiver =
        BeanCollector.builder()
            .build()
            .register()
            .host(jmxServiceURL().getHost())
            .port(jmxServiceURL().getPort())
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

  private OperatingSystemInfo mockResult(double usage) {
    var cpu = Mockito.mock(OperatingSystemInfo.class);
    Mockito.when(cpu.systemCpuLoad()).thenReturn(usage);
    return cpu;
  }
}
