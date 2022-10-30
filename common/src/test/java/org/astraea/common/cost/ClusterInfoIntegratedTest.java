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

import java.time.Duration;
import java.util.Optional;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ClusterInfoIntegratedTest extends RequireBrokerCluster {

  @Test
  void testQuery() {
    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(10)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      var clusterInfo =
          admin
              .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
              .toCompletableFuture()
              .join();

      // search by replica
      clusterInfo
          .replicas()
          .forEach(
              r ->
                  Assertions.assertNotEquals(
                      Optional.empty(), clusterInfo.replica(r.topicPartitionReplica())));
      clusterInfo
          .replicas()
          .forEach(
              r -> Assertions.assertNotEquals(0, clusterInfo.replicas(r.topicPartition()).size()));
      clusterInfo
          .replicas()
          .forEach(
              r -> Assertions.assertFalse(clusterInfo.replicaLeader(r.topicPartition()).isEmpty()));

      // search by topic
      clusterInfo
          .topics()
          .forEach(t -> Assertions.assertNotEquals(0, clusterInfo.replicas(t).size()));
      clusterInfo
          .topics()
          .forEach(t -> Assertions.assertNotEquals(0, clusterInfo.availableReplicas(t).size()));
      clusterInfo
          .topics()
          .forEach(t -> Assertions.assertNotEquals(0, clusterInfo.replicaLeaders(t).size()));
      clusterInfo
          .topics()
          .forEach(t -> Assertions.assertNotEquals(0, clusterInfo.replicaLeaders(0, t).size()));

      Assertions.assertNotEquals(0, clusterInfo.replicaLeaders().size());
    }
  }
}
