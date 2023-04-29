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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.metrics.ClusterBean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReplicaNumberCostTest {

  private static final ClusterInfo BASE =
      ClusterInfo.builder()
          .addNode(Set.of(1, 2, 3, 4, 5, 6))
          .addFolders(
              Map.ofEntries(
                  Map.entry(1, Set.of("/folder0", "/folder1", "/folder2")),
                  Map.entry(2, Set.of("/folder0", "/folder1", "/folder2")),
                  Map.entry(3, Set.of("/folder0", "/folder1", "/folder2")),
                  Map.entry(4, Set.of("/folder0", "/folder1", "/folder2")),
                  Map.entry(5, Set.of("/folder0", "/folder1", "/folder2")),
                  Map.entry(6, Set.of("/folder0", "/folder1", "/folder2"))))
          .build();
  private static final ClusterInfo BASE_1 =
      ClusterInfo.builder()
          .addNode(Set.of(1))
          .addFolders(Map.of(1, Set.of("/folder0", "/folder1", "/folder2")))
          .build();

  @Test
  void testClusterCost() {
    var cost = new ReplicaNumberCost();

    // (1,1,1,1,1,1)
    var evenCluster = ClusterInfo.builder(BASE).addTopic("topic", 6, (short) 1).build();
    Assertions.assertEquals(0, cost.clusterCost(evenCluster, ClusterBean.EMPTY).value());

    // (0)
    Assertions.assertEquals(0, cost.clusterCost(BASE_1, ClusterBean.EMPTY).value());

    // (0,0,0,0,0,0)
    Assertions.assertEquals(0, cost.clusterCost(BASE, ClusterBean.EMPTY).value());

    // (any)
    var singleNodeCluster =
        ClusterInfo.builder(BASE_1)
            .addTopic("topic", ThreadLocalRandom.current().nextInt(1, 100), (short) 1)
            .build();
    Assertions.assertEquals(0, cost.clusterCost(singleNodeCluster, ClusterBean.EMPTY).value());

    // (all >= 2, 0, 0, 0, 0, 0)
    var expandedCluster =
        ClusterInfo.builder(BASE_1)
            .addTopic("topic", ThreadLocalRandom.current().nextInt(2, 100), (short) 1)
            .addNode(Set.of(2, 3, 4, 5, 6))
            .build();
    Assertions.assertEquals(1, cost.clusterCost(expandedCluster, ClusterBean.EMPTY).value());

    // (40, 30, 20, 10)
    var skewCluster0 =
        ClusterInfo.builder()
            .addNode(Set.of(1))
            .addFolders(Map.of(1, Set.of("/folder")))
            .addTopic("topicA", 10, (short) 1)
            .addNode(Set.of(2))
            .addFolders(Map.of(2, Set.of("/folder")))
            .addTopic("topicB", 20, (short) 1)
            .addNode(Set.of(3))
            .addFolders(Map.of(3, Set.of("/folder")))
            .addTopic("topicC", 30, (short) 1)
            .addNode(Set.of(4))
            .addFolders(Map.of(4, Set.of("/folder")))
            .addTopic("topicD", 40, (short) 1)
            .build();
    var expected = (double) (40 - 10) / (40 + 30 + 20 + 10);
    Assertions.assertEquals(expected, cost.clusterCost(skewCluster0, ClusterBean.EMPTY).value());
  }

  @Test
  void testIntegerBalanceClusterCost() {
    // Allocate 7 replicas to 6 brokers, there is always a broker have an extra replica.
    // Given a (1,1,1,1,1,2) this should be considered as a balance case, so 0 should be return.
    for (int i = 7; i <= 11; i++) {
      var cost = new ReplicaNumberCost();
      var evenCluster = ClusterInfo.builder(BASE).addTopic("topic", i, (short) 1).build();
      Assertions.assertEquals(0, cost.clusterCost(evenCluster, ClusterBean.EMPTY).value());
    }
  }
}
