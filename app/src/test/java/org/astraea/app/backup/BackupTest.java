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
package org.astraea.app.backup;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Replica;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BackupTest {
  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testRestoreDistribution() {
    var topic1 = Utils.randomString();
    var topic2 = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin
          .creator()
          .topic(topic1)
          .numberOfPartitions(2)
          .numberOfReplicas((short) 2)
          .run()
          .toCompletableFuture()
          .join();
      admin
          .creator()
          .topic(topic2)
          .numberOfPartitions(2)
          .numberOfReplicas((short) 2)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      var clusterInfo = admin.clusterInfo(Set.of(topic1, topic2)).toCompletableFuture().join();
      admin.deleteTopics(Set.of(topic1, topic2)).toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));

      var backup = new Backup();
      backup.restoreDistribution(clusterInfo, SERVICE.bootstrapServers());
      Utils.sleep(Duration.ofSeconds(2));
      var restoredClusterInfo =
          admin.clusterInfo(Set.of(topic1, topic2)).toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));

      // Comparing with partial information between ClusterInfos. We do this because in KRaft world,
      // Kafka chooses a random broker node to report as the controller, resulting in different
      // Replica.broker.isController values.
      Assertions.assertEquals(clusterInfo.topics(), restoredClusterInfo.topics());
      Assertions.assertEquals(
          clusterInfo.topicPartitionReplicas(), restoredClusterInfo.topicPartitionReplicas());
      Assertions.assertEquals(
          clusterInfo.replicaLeaders().stream()
              .map(Replica::topicPartitionReplica)
              .collect(Collectors.toSet()),
          restoredClusterInfo.replicaLeaders().stream()
              .map(Replica::topicPartitionReplica)
              .collect(Collectors.toSet()));
    }
  }
}
