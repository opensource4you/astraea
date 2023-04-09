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
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.DataRate;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfoBuilder;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NetworkIngressCostTest {

  @Test
  void testTwoNodesWithSameTopic() {
    var clusterInfo =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1, 2))
            .addFolders(
                Map.of(1, Set.of("/folder0", "/folder1"), 2, Set.of("/folder0", "/folder1")))
            .addTopic(
                "a",
                4,
                (short) 1,
                replica -> {
                  var size = 0;
                  if (replica.partition() == 0) size = 10;
                  else if (replica.partition() == 1) size = 40;
                  else if (replica.partition() == 2) size = 90;
                  else size = 60;
                  return Replica.builder(replica).size(size).build();
                })
            .build();
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1,
                List.of(
                    NetworkCostTest.bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "a",
                        DataRate.MB.of(60).perSecond().byteRate())),
                2,
                List.of(
                    NetworkCostTest.bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "a",
                        DataRate.MB.of(60).perSecond().byteRate()))));

    var networkCost = new NetworkIngressCost(Configuration.EMPTY);
    var partitionCost = networkCost.partitionCost(clusterInfo, clusterBean);
    var tpBrokerId =
        clusterInfo
            .replicaStream()
            .collect(Collectors.toMap(Replica::topicPartition, r -> r.nodeInfo().id()));

    var incompatiblePartitions = partitionCost.incompatibility();
    incompatiblePartitions.forEach(
        (tp, set) -> {
          Assertions.assertFalse(set.isEmpty());
          set.forEach(p -> Assertions.assertEquals(tpBrokerId.get(tp), tpBrokerId.get(p)));
        });
  }

  @Test
  void testOneNodeWithMultipleTopics() {
    var clusterInfo =
        ClusterInfoBuilder.builder()
            .addNode(Set.of(1))
            .addFolders(Map.of(1, Set.of("/folder0", "/folder1")))
            .addTopic(
                "a",
                2,
                (short) 1,
                replica -> {
                  var size = 0;
                  if (replica.partition() == 0) size = 20;
                  else size = 80;
                  return Replica.builder(replica).size(size).build();
                })
            .addTopic(
                "b",
                2,
                (short) 1,
                replica -> {
                  var size = 0;
                  if (replica.partition() == 0) size = 40;
                  else size = 60;
                  return Replica.builder(replica).size(size).build();
                })
            .build();
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1,
                List.of(
                    NetworkCostTest.bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "a",
                        DataRate.MB.of(100).perSecond().byteRate()),
                    NetworkCostTest.bandwidth(
                        ServerMetrics.Topic.BYTES_IN_PER_SEC,
                        "b",
                        DataRate.MB.of(100).perSecond().byteRate()))));

    var networkCost = new NetworkIngressCost(Configuration.EMPTY);
    var partitionCost = networkCost.partitionCost(clusterInfo, clusterBean);

    var incompatible = partitionCost.incompatibility();
    incompatible.forEach(
        (tp, set) -> {
          if (tp.topic().equals("a") && tp.partition() == 0) Assertions.assertEquals(3, set.size());
          else Assertions.assertEquals(1, set.size());
        });
  }
}
