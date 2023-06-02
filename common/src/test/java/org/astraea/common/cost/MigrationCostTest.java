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

import static org.astraea.common.cost.MigrationCost.replicaLeaderToAdd;
import static org.astraea.common.cost.MigrationCost.replicaLeaderToRemove;
import static org.astraea.common.cost.MigrationCost.replicaNumChanged;

import java.util.List;
import org.astraea.common.admin.ClusterInfoTest;
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
                .brokerId(0)
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
                .brokerId(1)
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
                .brokerId(0)
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
                .brokerId(1)
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
                .brokerId(2)
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
                .brokerId(1)
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
                .brokerId(0)
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
                .brokerId(2)
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
                .brokerId(0)
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
                .brokerId(1)
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
                .brokerId(0)
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
                .brokerId(1)
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
                .brokerId(2)
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
                .brokerId(1)
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
                .brokerId(0)
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
                .brokerId(2)
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
}
