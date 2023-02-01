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
package org.astraea.gui.tab.health;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.TopicConfigs;
import org.astraea.common.admin.TopicPartition;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HealthNodeTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testEmptyTopics() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(2).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      var result = HealthNode.badTopics(admin).toCompletableFuture().join();
      Assertions.assertNotEquals(0, result.size());
      Assertions.assertEquals(topic, result.get(0).get("topic"));
      Assertions.assertTrue((Boolean) result.get(0).get("empty"));
    }
  }

  @Test
  void testUnavailablePartitions() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 2)
          .configs(Map.of(TopicConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(2));

      // reduce the number of replicas
      admin
          .moveToBrokers(
              Map.of(
                  TopicPartition.of(topic, 0),
                  List.of(SERVICE.dataFolders().keySet().iterator().next())))
          .toCompletableFuture()
          .join();

      Utils.sleep(Duration.ofSeconds(2));

      var result = HealthNode.unavailablePartitions(admin).toCompletableFuture().join();
      Assertions.assertEquals(1, result.size());
      Assertions.assertEquals(topic, result.get(0).get("topic"));
      Assertions.assertFalse((boolean) result.get(0).get("writable"));
    }
  }
}
