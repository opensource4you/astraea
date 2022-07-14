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
package org.astraea.app.partitioner.smooth;

import java.util.List;
import java.util.Map;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SmoothWeightRoundRobinTest {
  @Test
  void testGetAndChoose() {
    var topic = "test";
    var smoothWeight = new SmoothWeightRoundRobin(Map.of(1, 5.0, 2, 3.0, 3, 1.0));
    var testCluster = mockResult();

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
    var testCluster = mockResult();

    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(2, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(3, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(2, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(3, smoothWeight.getAndChoose(topic, testCluster));
    Assertions.assertEquals(1, smoothWeight.getAndChoose(topic, testCluster));
  }

  ClusterInfo mockResult() {
    var re1 = Mockito.mock(ReplicaInfo.class);
    var node1 = Mockito.mock(NodeInfo.class);
    Mockito.when(re1.nodeInfo()).thenReturn(node1);
    Mockito.when(node1.id()).thenReturn(1);

    var re2 = Mockito.mock(ReplicaInfo.class);
    var node2 = Mockito.mock(NodeInfo.class);
    Mockito.when(re2.nodeInfo()).thenReturn(node2);
    Mockito.when(node2.id()).thenReturn(2);

    var re3 = Mockito.mock(ReplicaInfo.class);
    var node3 = Mockito.mock(NodeInfo.class);
    Mockito.when(re3.nodeInfo()).thenReturn(node3);
    Mockito.when(node3.id()).thenReturn(3);
    var clusterInfo = Mockito.mock(ClusterInfo.class);
    Mockito.when(clusterInfo.availableReplicaLeaders(Mockito.anyString()))
        .thenReturn(List.of(re1, re2, re3));
    return clusterInfo;
  }
}
