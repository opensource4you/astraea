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
package org.astraea.common.admin;

import java.time.Duration;
import java.util.Collection;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SomePartitionOfflineTest extends RequireBrokerCluster {
  @Test
  void somePartitionsOffline() {
    String topicName1 = "testOfflineTopic-1";
    String topicName2 = "testOfflineTopic-2";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName1).numberOfPartitions(4).numberOfReplicas((short) 1).create();
      admin.creator().topic(topicName2).numberOfPartitions(4).numberOfReplicas((short) 1).create();
      // wait for topic creation
      Utils.sleep(Duration.ofSeconds(3));
      var replicaOnBroker0 =
          admin.replicas(admin.topicNames()).stream()
              .filter(replica -> replica.nodeInfo().id() == 0)
              .collect(
                  Collectors.groupingBy(
                      replica -> TopicPartition.of(replica.topic(), replica.partition())));
      replicaOnBroker0.forEach((tp, replica) -> Assertions.assertFalse(replica.get(0).isOffline()));
      closeBroker(0);
      Assertions.assertNull(logFolders().get(0));
      Assertions.assertNotNull(logFolders().get(1));
      Assertions.assertNotNull(logFolders().get(2));
      var offlineReplicaOnBroker0 =
          admin.replicas(admin.topicNames()).stream()
              .filter(replica -> replica.nodeInfo().id() == 0)
              .collect(
                  Collectors.groupingBy(
                      replica -> TopicPartition.of(replica.topic(), replica.partition())));
      offlineReplicaOnBroker0.values().stream()
          .flatMap(Collection::stream)
          .forEach(
              replica -> {
                Assertions.assertTrue(replica.isOffline());
                Assertions.assertEquals(-1, replica.size());
                Assertions.assertEquals(-1, replica.lag());
                Assertions.assertNull(replica.path());
              });
    }
  }
}
