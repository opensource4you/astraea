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

import static org.astraea.common.cost.CostUtils.changedRecordSizeOverflow;
import static org.astraea.common.cost.MigrationCost.recordSizeToFetch;
import static org.astraea.common.cost.MigrationCost.recordSizeToSync;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Config;
import org.astraea.common.admin.Replica;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CostUtilsTest {

  @Test
  void testChangedRecordSizeOverflow() {
    var limit = 1600;
    var moveInResult = recordSizeToSync(beforeClusterInfo(), afterClusterInfo());
    Assertions.assertEquals(3, moveInResult.size());
    Assertions.assertEquals(0, moveInResult.get(0));
    Assertions.assertEquals(1000, moveInResult.get(1));
    Assertions.assertEquals(100 + 500, moveInResult.get(2));

    var moveOutResult = recordSizeToFetch(beforeClusterInfo(), afterClusterInfo());
    Assertions.assertEquals(3, moveOutResult.size());
    Assertions.assertEquals(100 + 500, moveOutResult.get(0));
    Assertions.assertEquals(0, moveOutResult.get(1));
    Assertions.assertEquals(1000, moveOutResult.get(2));

    var totalResult =
        changedRecordSizeOverflow(beforeClusterInfo(), afterClusterInfo(), ignored -> true, limit);
    var overflowResult =
        changedRecordSizeOverflow(
            beforeClusterInfo(), afterClusterInfo(), ignored -> true, limit - 100);
    Assertions.assertFalse(totalResult);
    Assertions.assertTrue(overflowResult);
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

    var dataPath =
        Map.of(
            0,
            List.of(new Broker.DataFolder("/path0", Map.of(), Map.of())),
            1,
            List.of(new Broker.DataFolder("/path0", Map.of(), Map.of())),
            2,
            List.of(
                new Broker.DataFolder("/path0", Map.of(), Map.of()),
                new Broker.DataFolder("/path1", Map.of(), Map.of())));
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
                        brokerId,
                        "",
                        22222,
                        false,
                        Config.EMPTY,
                        dataPath.get(brokerId),
                        Set.of(),
                        Set.of()))
            .collect(Collectors.toList()),
        Map.of(),
        replicas);
  }

  private static ClusterInfo afterClusterInfo() {

    var dataPath =
        Map.of(
            0,
            List.of(new Broker.DataFolder("/path0", Map.of(), Map.of())),
            1,
            List.of(new Broker.DataFolder("/path0", Map.of(), Map.of())),
            2,
            List.of(
                new Broker.DataFolder("/path0", Map.of(), Map.of()),
                new Broker.DataFolder("/path1", Map.of(), Map.of())));
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
                        brokerId,
                        "",
                        22222,
                        false,
                        Config.EMPTY,
                        dataPath.get(brokerId),
                        Set.of(),
                        Set.of()))
            .collect(Collectors.toList()),
        Map.of(),
        replicas);
  }
}
