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
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.ClusterBean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RecordSizeCostTest {

  private final RecordSizeCost function = new RecordSizeCost();
  ;

  private final ClusterInfo clusterInfo =
      ClusterInfo.of(
          "fake",
          List.of(Broker.of(0, "aa", 22), Broker.of(1, "aa", 22), Broker.of(2, "aa", 22)),
          Map.of(),
          List.of(
              Replica.builder()
                  .topic("topic")
                  .partition(0)
                  .broker(Broker.of(1, "aa", 22))
                  .size(100)
                  .path("/tmp/aa")
                  .buildLeader(),
              Replica.builder()
                  .topic("topic")
                  .partition(0)
                  .broker(Broker.of(2, "aa", 22))
                  .size(99)
                  .path("/tmp/aa")
                  .buildInSyncFollower(),
              Replica.builder()
                  .topic("topic")
                  .partition(1)
                  .broker(Broker.of(1, "aa", 22))
                  .size(11)
                  .path("/tmp/aa")
                  .buildLeader()));

  @Test
  void testBrokerCost() {
    var result = function.brokerCost(clusterInfo, ClusterBean.EMPTY);
    Assertions.assertEquals(3, result.value().size());
    Assertions.assertEquals(111, result.value().get(1));
    Assertions.assertEquals(99, result.value().get(2));
  }

  @Test
  void testPartitionCost() {
    var result = function.partitionCost(clusterInfo, ClusterBean.EMPTY);
    Assertions.assertEquals(2, result.value().size());
    Assertions.assertEquals(100, result.value().get(TopicPartition.of("topic", 0)));
    Assertions.assertEquals(11, result.value().get(TopicPartition.of("topic", 1)));
  }
}
