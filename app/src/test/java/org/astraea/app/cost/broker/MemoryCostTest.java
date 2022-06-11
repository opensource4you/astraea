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

import java.lang.management.MemoryUsage;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.FakeClusterInfo;
import org.astraea.app.cost.NodeInfo;
import org.astraea.app.cost.Normalizer;
import org.astraea.app.cost.ReplicaInfo;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.java.HasJvmMemory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MemoryCostTest {
  @Test
  void testCost() throws InterruptedException {
    var jvmMemory1 = mockResult(40L, 100L);
    var jvmMemory2 = mockResult(81L, 100L);
    var jvmMemory3 = mockResult(80L, 150L);

    Collection<HasBeanObject> broker1 = List.of(jvmMemory1);
    Collection<HasBeanObject> broker2 = List.of(jvmMemory2);
    Collection<HasBeanObject> broker3 = List.of(jvmMemory3);
    ClusterInfo clusterInfo =
        new FakeClusterInfo() {
          @Override
          public Map<Integer, Collection<HasBeanObject>> allBeans() {
            return Map.of(1, broker1, 2, broker2, 3, broker3);
          }

          @Override
          public Set<String> topics() {
            return Set.of("t");
          }

          @Override
          public List<ReplicaInfo> availableReplicas(String topic) {
            return List.of(
                ReplicaInfo.of("t", 0, NodeInfo.of(1, "host1", 9092), true, false, false),
                ReplicaInfo.of("t", 0, NodeInfo.of(2, "host2", 9092), false, true, false),
                ReplicaInfo.of("t", 0, NodeInfo.of(3, "host3", 9092), false, true, false));
          }
        };

    var memoryCost = new MemoryCost();
    var scores = memoryCost.brokerCost(clusterInfo).normalize(Normalizer.TScore()).value();
    Assertions.assertEquals(0.39, scores.get(1));
    Assertions.assertEquals(0.63, scores.get(2));
    Assertions.assertEquals(0.47, scores.get(3));

    Thread.sleep(1000);
    jvmMemory1 = mockResult(50L, 150L);
    jvmMemory2 = mockResult(30L, 100L);
    jvmMemory3 = mockResult(80L, 150L);

    Collection<HasBeanObject> broker12 = List.of(jvmMemory1);
    Collection<HasBeanObject> broker22 = List.of(jvmMemory2);
    Collection<HasBeanObject> broker32 = List.of(jvmMemory3);
    ClusterInfo clusterInfo2 =
        new FakeClusterInfo() {
          @Override
          public Map<Integer, Collection<HasBeanObject>> allBeans() {
            return Map.of(1, broker12, 2, broker22, 3, broker32);
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
    scores = memoryCost.brokerCost(clusterInfo2).normalize(Normalizer.TScore()).value();
    Assertions.assertEquals(0.36, scores.get(1));
    Assertions.assertEquals(0.58, scores.get(2));
    Assertions.assertEquals(0.56, scores.get(3));
  }

  private HasJvmMemory mockResult(long used, long max) {
    var jvmMemory = Mockito.mock(HasJvmMemory.class);
    var memoryUsage = Mockito.mock(MemoryUsage.class);
    Mockito.when(jvmMemory.heapMemoryUsage()).thenReturn(memoryUsage);
    Mockito.when(memoryUsage.getUsed()).thenReturn(used);
    Mockito.when(memoryUsage.getMax()).thenReturn(max);
    return jvmMemory;
  }
}
