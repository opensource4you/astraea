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
package org.astraea.common.partitioner.smooth;

import java.util.List;
import java.util.Map;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SmoothWeightRoundRobinTest {
  @Test
  void testGetAndChoose() {
    var topic = "test";
    var smoothWeight = new SmoothWeightRoundRobin(Map.of(1, 5.0, 2, 3.0, 3, 1.0));
    var testCluster = clusterInfo();

    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(2, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(3, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(2, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(3, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
  }

  @Test
  void testPartOfBrokerGetAndChoose() {
    var topic = "test";
    var smoothWeight = new SmoothWeightRoundRobin(Map.of(1, 5.0, 2, 3.0, 3, 1.0, 4, 1.0));
    var testCluster = clusterInfo();

    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(2, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(3, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(2, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(3, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
  }

  ClusterInfo<Replica> clusterInfo() {
    var nodes =
        List.of(
            NodeInfo.of(1, "host", 1111),
            NodeInfo.of(2, "host", 1111),
            NodeInfo.of(3, "host", 1111));
    return ClusterInfo.of(
        "fake",
        nodes,
        List.of(
            Replica.builder()
                .topic("test")
                .partition(1)
                .nodeInfo(nodes.get(0))
                .path("/tmp/aa")
                .buildLeader(),
            Replica.builder()
                .topic("test")
                .partition(2)
                .nodeInfo(nodes.get(1))
                .path("/tmp/aa")
                .buildLeader(),
            Replica.builder()
                .topic("test")
                .partition(3)
                .nodeInfo(nodes.get(2))
                .path("/tmp/aa")
                .buildLeader()));
  }
}
