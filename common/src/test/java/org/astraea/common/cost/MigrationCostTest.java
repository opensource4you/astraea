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

import static org.astraea.common.cost.MigrationCost.changedRecordSizeOverflow;
import static org.astraea.common.cost.MigrationCost.recordSizeToFetch;
import static org.astraea.common.cost.MigrationCost.recordSizeToSync;
import static org.astraea.common.cost.MigrationCost.replicaLeaderToAdd;
import static org.astraea.common.cost.MigrationCost.replicaLeaderToRemove;
import static org.astraea.common.cost.MigrationCost.replicaNumChanged;

import java.util.List;
import java.util.Map;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoTest;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MigrationCostTest {

  @Test
  void testChangedReplicaLeaderNumber() {
    var before =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build());
    var after =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build());
    /*
       before:
         topic1-0 : 0,1
         topic1-1 : 0,1
       after:
         topic1-0 : 2,1
         topic1-1 : 0,2
       leader migrate out:
         0: 1
         1: 0
         2: 0
       leader migrate in:
         0: 0
         1: 0
         2: 1
    */

    var beforeClusterInfo = ClusterInfoTest.of(before);
    var afterClusterInfo = ClusterInfoTest.of(after);
    var changedReplicaLeaderInCount = replicaLeaderToAdd(beforeClusterInfo, afterClusterInfo);
    var changedReplicaLeaderOutCount = replicaLeaderToRemove(beforeClusterInfo, afterClusterInfo);
    Assertions.assertEquals(3, changedReplicaLeaderInCount.size());
    Assertions.assertEquals(1, changedReplicaLeaderInCount.get(0));
    Assertions.assertEquals(0, changedReplicaLeaderInCount.get(1));
    Assertions.assertEquals(0, changedReplicaLeaderInCount.get(2));
    Assertions.assertEquals(0, changedReplicaLeaderOutCount.get(0));
    Assertions.assertEquals(0, changedReplicaLeaderOutCount.get(1));
    Assertions.assertEquals(1, changedReplicaLeaderOutCount.get(2));
  }

  @Test
  void testChangedReplicaNumber() {
    var before =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build());
    var after =
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(true)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .lag(-1)
                .size(-1)
                .isLeader(false)
                .isSync(true)
                .isFuture(false)
                .isOffline(false)
                .isPreferredLeader(false)
                .path("")
                .build());
    var beforeClusterInfo = ClusterInfoTest.of(before);
    var afterClusterInfo = ClusterInfoTest.of(after);
    var changedReplicaCount = replicaNumChanged(beforeClusterInfo, afterClusterInfo);
    Assertions.assertEquals(3, changedReplicaCount.size(), changedReplicaCount.toString());
    Assertions.assertTrue(changedReplicaCount.containsKey(0));
    Assertions.assertTrue(changedReplicaCount.containsKey(1));
    Assertions.assertTrue(changedReplicaCount.containsKey(2));
    Assertions.assertEquals(-1, changedReplicaCount.get(0));
    Assertions.assertEquals(-1, changedReplicaCount.get(1));
    Assertions.assertEquals(2, changedReplicaCount.get(2));
  }

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
    return ClusterInfo.of(
        "fake",
        List.of(NodeInfo.of(0, "aa", 22), NodeInfo.of(1, "aa", 22), NodeInfo.of(2, "aa", 22)),
        Map.of(),
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(100)
                .isLeader(true)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(99)
                .isLeader(false)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(500)
                .isLeader(true)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(499)
                .isLeader(false)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(1000)
                .isLeader(true)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(1000)
                .isLeader(false)
                .build()));
  }

  private static ClusterInfo afterClusterInfo() {
    return ClusterInfo.of(
        "fake",
        List.of(NodeInfo.of(0, "aa", 22), NodeInfo.of(1, "aa", 22), NodeInfo.of(2, "aa", 22)),
        Map.of(),
        List.of(
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(100)
                .isLeader(true)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(0)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(99)
                .isLeader(false)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(500)
                .isLeader(true)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(1)
                .nodeInfo(NodeInfo.of(2, "broker0", 1111))
                .size(500)
                .isLeader(false)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(1, "broker0", 1111))
                .size(1000)
                .isLeader(true)
                .build(),
            Replica.builder()
                .topic("topic1")
                .partition(2)
                .nodeInfo(NodeInfo.of(0, "broker0", 1111))
                .size(1000)
                .isLeader(false)
                .build()));
  }
}
