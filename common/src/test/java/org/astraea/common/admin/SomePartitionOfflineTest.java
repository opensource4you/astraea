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
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SomePartitionOfflineTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void somePartitionsOffline() {
    String topicName1 = "testOfflineTopic-1";
    String topicName2 = "testOfflineTopic-2";
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin
          .creator()
          .topic(topicName1)
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      admin
          .creator()
          .topic(topicName2)
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .run()
          .toCompletableFuture()
          .join();
      // wait for topic creation
      Utils.sleep(Duration.ofSeconds(3));
      var replicaOnBroker0 =
          admin
              .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
              .toCompletableFuture()
              .join()
              .replicaStream()
              .filter(replica -> replica.brokerId() == 0)
              .collect(
                  Collectors.groupingBy(
                      replica -> TopicPartition.of(replica.topic(), replica.partition())));
      replicaOnBroker0.forEach((tp, replica) -> Assertions.assertFalse(replica.get(0).isOffline()));
      SERVICE.close(0);
      Assertions.assertNull(SERVICE.dataFolders().get(0));
      Assertions.assertNotNull(SERVICE.dataFolders().get(1));
      Assertions.assertNotNull(SERVICE.dataFolders().get(2));
      var offlineReplicaOnBroker0 =
          admin
              .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
              .toCompletableFuture()
              .join()
              .replicaStream()
              .filter(replica -> replica.brokerId() == 0)
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
