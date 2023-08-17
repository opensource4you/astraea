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

import static org.astraea.common.cost.BrokerDiskSpaceCost.brokerDiskUsageSizeOverflow;
import static org.astraea.common.cost.BrokerDiskSpaceCost.brokerPathDiskUsageSizeOverflow;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Config;
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
                .brokerId(0)
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .brokerId(1)
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .brokerId(0)
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .brokerId(1)
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .brokerId(0)
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .brokerId(2)
                .size(dataSize.bytes())
                .path("/path0")
                .build());
    var after =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .brokerId(2)
                .size(dataSize.bytes())
                .path("/path1")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .brokerId(1)
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .brokerId(0)
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .brokerId(2)
                .size(dataSize.bytes())
                .path("/path1")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .brokerId(0)
                .size(dataSize.bytes())
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .brokerId(2)
                .size(dataSize.bytes())
                .path("/path0")
                .build());
    var beforeClusterInfo = of(before);
    var afterClusterInfo = of(after);
    var brokerConfig =
        new Configuration(
            Map.of(BrokerDiskSpaceCost.BROKER_COST_LIMIT_KEY, "0:1500MB,1:1000MB,2:1500MB"));
    var brokerOverflowConfig =
        new Configuration(
            Map.of(BrokerDiskSpaceCost.BROKER_COST_LIMIT_KEY, "0:1300MB,1:1000MB,2:1500MB"));
    var pathConfig =
        new Configuration(
            Map.of(
                BrokerDiskSpaceCost.BROKER_PATH_COST_LIMIT_KEY,
                "0-/path0:1500MB,1-/path0:1000MB,2-/path0:1500MB,2-/path1:1000MB"));
    var pathOverflowConfig =
        new Configuration(
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

  @Test
  void testBrokerDiskUsageSizeOverflow() {
    var limit =
        Map.of(
            0, DataSize.Byte.of(1600),
            1, DataSize.Byte.of(1598),
            2, DataSize.Byte.of(1600));
    var overFlowLimit =
        Map.of(
            0, DataSize.Byte.of(1600),
            1, DataSize.Byte.of(1598),
            2, DataSize.Byte.of(1500));
    var totalResult = brokerDiskUsageSizeOverflow(beforeClusterInfo(), afterClusterInfo(), limit);
    var overflowResult =
        brokerDiskUsageSizeOverflow(beforeClusterInfo(), afterClusterInfo(), overFlowLimit);
    Assertions.assertFalse(totalResult);
    Assertions.assertTrue(overflowResult);
  }

  @Test
  void testBrokerPathDiskUsageSizeOverflow() {
    var limit =
        Map.of(
            new BrokerDiskSpaceCost.BrokerPath(0, "/path0"),
            DataSize.Byte.of(1600),
            new BrokerDiskSpaceCost.BrokerPath(1, "/path0"),
            DataSize.Byte.of(1598),
            new BrokerDiskSpaceCost.BrokerPath(2, "/path0"),
            DataSize.Byte.of(1600),
            new BrokerDiskSpaceCost.BrokerPath(2, "/path1"),
            DataSize.Byte.of(600));
    var overFlowLimit =
        Map.of(
            new BrokerDiskSpaceCost.BrokerPath(0, "/path0"), DataSize.Byte.of(1600),
            new BrokerDiskSpaceCost.BrokerPath(1, "/path0"), DataSize.Byte.of(1598),
            new BrokerDiskSpaceCost.BrokerPath(2, "/path0"), DataSize.Byte.of(1600),
            new BrokerDiskSpaceCost.BrokerPath(2, "/path1"), DataSize.Byte.of(500));
    var totalResult =
        brokerPathDiskUsageSizeOverflow(beforeClusterInfo(), afterClusterInfo(), limit);
    var overflowResult =
        brokerPathDiskUsageSizeOverflow(beforeClusterInfo(), afterClusterInfo(), overFlowLimit);
    Assertions.assertFalse(totalResult);
    Assertions.assertTrue(overflowResult);
  }

  public static ClusterInfo of(List<Replica> replicas) {
    var dataPath = Map.of(0, Set.of("/path0"), 1, Set.of("/path0"), 2, Set.of("/path0", "/path1"));
    return ClusterInfo.of(
        "fake",
        replicas.stream()
            .map(Replica::brokerId)
            .distinct()
            .map(
                brokerId ->
                    new Broker(
                        brokerId, "", 2222, false, Config.EMPTY, dataPath.get(brokerId), List.of()))
            .collect(Collectors.toList()),
        Map.of(),
        replicas);
  }

  /*
  before distribution:
      p0: 0,1
      p1: 0,1
      p2: 2,0
  after distribution:
      p0: 2,1
      p1: 0,2
      p2: 1,0
  leader log size:
      p0: 100
      p1: 500
      p2  1000
   */
  private static ClusterInfo beforeClusterInfo() {

    var dataPath = Map.of(0, Set.of("/path0"), 1, Set.of("/path0"), 2, Set.of("/path0", "/path1"));
    var replicas =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .brokerId(0)
                .size(100)
                .isLeader(true)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .brokerId(1)
                .size(99)
                .isLeader(false)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .brokerId(0)
                .size(500)
                .isLeader(true)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .brokerId(1)
                .size(499)
                .isLeader(false)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .brokerId(2)
                .size(1000)
                .isLeader(true)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .brokerId(0)
                .size(1000)
                .isLeader(false)
                .path("/path0")
                .build());
    return ClusterInfo.of(
        "fake",
        replicas.stream()
            .map(Replica::brokerId)
            .distinct()
            .map(
                brokerId ->
                    new Broker(
                        brokerId, "", 2222, false, Config.EMPTY, dataPath.get(brokerId), List.of()))
            .collect(Collectors.toList()),
        Map.of(),
        replicas);
  }

  private static ClusterInfo afterClusterInfo() {
    var dataPath = Map.of(0, Set.of("/path0"), 1, Set.of("/path0"), 2, Set.of("/path0", "/path1"));
    var replicas =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .brokerId(2)
                .size(100)
                .isLeader(true)
                .path("/path1")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .brokerId(1)
                .size(99)
                .isLeader(false)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .brokerId(0)
                .size(500)
                .isLeader(true)
                .path("/path0")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .brokerId(2)
                .size(500)
                .isLeader(false)
                .path("/path1")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .brokerId(1)
                .size(1000)
                .isLeader(true)
                .path("/path1")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .brokerId(0)
                .size(1000)
                .isLeader(false)
                .path("/path0")
                .build());
    return ClusterInfo.of(
        "fake",
        replicas.stream()
            .map(Replica::brokerId)
            .distinct()
            .map(
                brokerId ->
                    new Broker(
                        brokerId, "", 2222, false, Config.EMPTY, dataPath.get(brokerId), List.of()))
            .collect(Collectors.toList()),
        Map.of(),
        replicas);
  }
}
