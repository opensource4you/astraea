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
package org.astraea.app.admin;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.astraea.app.common.Utils;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ClusterInfoWithOfflineNodeTest extends RequireBrokerCluster {
  @Test
  void testClusterInfoWithOfflineNode() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topicName = "ClusterInfo_Offline_" + Utils.randomString();
      var partitionCount = 30;
      var replicaCount = (short) 3;
      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(partitionCount)
          .numberOfReplicas(replicaCount)
          .create();
      Utils.sleep(Duration.ofSeconds(3));

      // before node offline
      var before = admin.clusterInfo(Set.of(topicName));
      Assertions.assertEquals(
          partitionCount * replicaCount,
          before.replicas(topicName).stream().filter(x -> !x.isOffline()).count());
      Assertions.assertEquals(
          partitionCount * replicaCount, before.availableReplicas(topicName).size());
      Assertions.assertEquals(partitionCount, before.replicaLeaders(topicName).size());

      // act
      int brokerToClose = ThreadLocalRandom.current().nextInt(0, 3);
      closeBroker(brokerToClose);
      Utils.sleep(Duration.ofSeconds(1));

      // after node offline
      var after = admin.clusterInfo(Set.of(topicName));
      Assertions.assertEquals(
          partitionCount * (replicaCount - 1),
          after.replicas(topicName).stream().filter(x -> !x.isOffline()).count());
      Assertions.assertEquals(
          partitionCount * (replicaCount - 1), after.availableReplicas(topicName).size());
      Assertions.assertEquals(
          partitionCount,
          after.replicaLeaders(topicName).size(),
          "One of the rest replicas should take over the leadership");
      Assertions.assertTrue(
          after.availableReplicas(topicName).stream()
              .allMatch(x -> x.nodeInfo().id() != brokerToClose));
      Assertions.assertTrue(
          after.replicaLeaders(topicName).stream()
              .allMatch(x -> x.nodeInfo().id() != brokerToClose));
      Assertions.assertTrue(
          after.replicas(topicName).stream()
              .filter(ReplicaInfo::isOffline)
              .allMatch(x -> x.nodeInfo().id() == brokerToClose));
      Assertions.assertTrue(
          after.replicas(topicName).stream()
              .filter(x -> !x.isOffline())
              .allMatch(x -> x.nodeInfo().id() != brokerToClose));
    }
  }
}
