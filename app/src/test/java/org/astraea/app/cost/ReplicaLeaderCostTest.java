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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicaLeaderCostTest {

  @Test
  void testCost() {
    var replicas =
        List.of(
            ReplicaInfo.of("topic", 0, NodeInfo.of(10, "broker0", 1111), true, true, true),
            ReplicaInfo.of("topic", 0, NodeInfo.of(10, "broker0", 1111), true, true, true),
            ReplicaInfo.of("topic", 0, NodeInfo.of(11, "broker1", 1111), true, true, true));
    var function = new ReplicaLeaderCost();
    var cost = function.brokerCost(replicas.stream());
    Assertions.assertTrue(cost.value().containsKey(10));
    Assertions.assertTrue(cost.value().containsKey(11));
    Assertions.assertEquals(2, cost.value().size());
    Assertions.assertTrue(cost.value().get(10) > cost.value().get(11));
  }
}
