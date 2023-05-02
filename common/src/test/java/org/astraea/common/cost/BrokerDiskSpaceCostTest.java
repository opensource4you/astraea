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
import java.util.stream.Collectors;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.ClusterBean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BrokerDiskSpaceCostTest {

  @Test
  void testMoveCosts() {
    var dataSize = DataSize.of("500MB");
    /*
       replica distribution:
           p0: 0,1 -> 2,1
           p1: 0,1 -> 0,2
           p2: 0,2 -> 0,2
       replicas during migrated per broker:
            0: p0,p1,p2
            1: p0,p1
            2: p0,p1,p2
    */
    var before =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(dataSize.bytes())
                .path("/path0")
                .build());
    var after =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(dataSize.bytes())
                .path("/path1")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(dataSize.bytes())
                .path("/path1")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(dataSize.bytes())
                .path("/path0")
                .build());
    var beforeClusterInfo = of(before);
    var afterClusterInfo = of(after);
    var brokerConfig =
        Configuration.of(
            Map.of(BrokerDiskSpaceCost.BROKER_COST_LIMIT_KEY, "0:1500MB,1:1000MB,2:1500MB"));
    var brokerOverflowConfig =
        Configuration.of(
            Map.of(BrokerDiskSpaceCost.BROKER_COST_LIMIT_KEY, "0:1300MB,1:1000MB,2:1500MB"));
    var pathConfig =
        Configuration.of(
            Map.of(
                BrokerDiskSpaceCost.BROKER_PATH_COST_LIMIT_KEY,
                "0-/path0:1500MB,1-/path0:1000MB,2-/path0:1500MB,2-/path1:1000MB"));
    var pathOverflowConfig =
        Configuration.of(
            Map.of(
                BrokerDiskSpaceCost.BROKER_PATH_COST_LIMIT_KEY,
                "0-/path0:1500MB,1-/path0:1000MB,2-/path0:1500MB,2-/path1:900MB"));
    // set broker limit no overflow
    var cf0 = new BrokerDiskSpaceCost(brokerConfig);
    var moveCost0 = cf0.moveCost(beforeClusterInfo, afterClusterInfo, ClusterBean.EMPTY);
    // set broker limit and overflow
    var cf1 = new BrokerDiskSpaceCost(brokerOverflowConfig);
    var moveCost1 = cf1.moveCost(beforeClusterInfo, afterClusterInfo, ClusterBean.EMPTY);
    // set path limit no overflow
    var cf2 = new BrokerDiskSpaceCost(pathConfig);
    var moveCost2 = cf2.moveCost(beforeClusterInfo, afterClusterInfo, ClusterBean.EMPTY);
    // set path limit and overflow
    var cf3 = new BrokerDiskSpaceCost(pathOverflowConfig);
    var moveCost3 = cf3.moveCost(beforeClusterInfo, afterClusterInfo, ClusterBean.EMPTY);

    Assertions.assertFalse(moveCost0.overflow());
    Assertions.assertTrue(moveCost1.overflow());
    Assertions.assertFalse(moveCost2.overflow());
    Assertions.assertTrue(moveCost3.overflow());
  }

  public static ClusterInfo of(List<Replica> replicas) {
    var dataPath =
        Map.of(
            0,
            Map.of("/path0", new DescribeLogDirsResponse.LogDirInfo(null, Map.of())),
            1,
            Map.of("/path0", new DescribeLogDirsResponse.LogDirInfo(null, Map.of())),
            2,
            Map.of(
                "/path0",
                new DescribeLogDirsResponse.LogDirInfo(null, Map.of()),
                "/path1",
                new DescribeLogDirsResponse.LogDirInfo(null, Map.of())));
    return ClusterInfo.of(
        "fake",
        replicas.stream()
            .map(Replica::nodeInfo)
            .distinct()
            .map(
                nodeInfo ->
                    Broker.of(
                        false,
                        new Node(nodeInfo.id(), "", nodeInfo.port()),
                        Map.of(),
                        dataPath.get(nodeInfo.id()),
                        List.of()))
            .collect(Collectors.toList()),
        Map.of(),
        replicas);
  }
}
