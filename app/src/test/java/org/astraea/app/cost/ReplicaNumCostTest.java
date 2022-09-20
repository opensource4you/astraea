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
package org.astraea.app.cost;

import java.util.List;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.cost.ReplicaNumCost;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReplicaNumCostTest {
  @Test
  void testMoveCost() {
    var costFunction = new ReplicaNumCost();
    var before =
        List.of(
            Replica.of(
                "topic1",
                0,
                NodeInfo.of(0, "broker0", 1111),
                -1,
                -1,
                true,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "topic1",
                0,
                NodeInfo.of(1, "broker0", 1111),
                -1,
                -1,
                false,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "topic1",
                1,
                NodeInfo.of(0, "broker0", 1111),
                -1,
                -1,
                true,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "topic1",
                1,
                NodeInfo.of(1, "broker0", 1111),
                -1,
                -1,
                false,
                true,
                false,
                false,
                false,
                ""));
    var after =
        List.of(
            Replica.of(
                "topic1",
                0,
                NodeInfo.of(2, "broker0", 1111),
                -1,
                -1,
                true,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "topic1",
                0,
                NodeInfo.of(1, "broker0", 1111),
                -1,
                -1,
                false,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "topic1",
                1,
                NodeInfo.of(0, "broker0", 1111),
                -1,
                -1,
                true,
                true,
                false,
                false,
                false,
                ""),
            Replica.of(
                "topic1",
                1,
                NodeInfo.of(2, "broker0", 1111),
                -1,
                -1,
                false,
                true,
                false,
                false,
                false,
                ""));
    var beforeClusterInfo = ClusterInfo.of(before);
    var afterClusterInfo = ClusterInfo.of(after);
    var movecost = costFunction.moveCost(beforeClusterInfo, afterClusterInfo, ClusterBean.EMPTY);
    Assertions.assertEquals(2, movecost.totalCost());
    Assertions.assertEquals(3, movecost.changes().size());
    Assertions.assertTrue(movecost.changes().containsKey(0));
    Assertions.assertTrue(movecost.changes().containsKey(1));
    Assertions.assertTrue(movecost.changes().containsKey(2));
    Assertions.assertEquals(-1, movecost.changes().get(0));
    Assertions.assertEquals(-1, movecost.changes().get(1));
    Assertions.assertEquals(2, movecost.changes().get(2));
  }
}
