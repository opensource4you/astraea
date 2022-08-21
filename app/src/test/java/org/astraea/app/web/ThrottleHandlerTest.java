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
package org.astraea.app.web;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.Utils;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Test;

public class ThrottleHandlerTest extends RequireBrokerCluster {

  @Test
  void testThrottleHandler1() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var topicName = Utils.randomString();
      admin.creator().topic(topicName).numberOfPartitions(3).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));
      admin
          .replicationThrottler()
          .throttle(topicName)
          .ingress(DataRate.MiB.of(500).perSecond())
          .egress(DataRate.MiB.of(500).perSecond())
          .apply();
      Utils.sleep(Duration.ofSeconds(1));

      System.out.println(handler.get(Optional.empty(), Map.of()).json());
    }
  }

  @Test
  void testThrottleHandler2() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ThrottleHandler(admin);
      var topicName = Utils.randomString();
      admin.creator().topic(topicName).numberOfPartitions(3).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(1));
      admin
          .replicationThrottler()
          .throttle(TopicPartitionReplica.of(topicName, 0, 0))
          .throttle(TopicPartitionReplica.of(topicName, 0, 1))
          .throttle(TopicPartitionReplica.of(topicName, 0, 2))
          .throttle(TopicPartitionReplica.of(topicName, 1, 0))
          .throttle(TopicPartitionReplica.of(topicName, 1, 1))
          .throttle(TopicPartitionReplica.of(topicName, 1, 2))
          .throttle(TopicPartitionReplica.of(topicName, 2, 0))
          .throttle(TopicPartitionReplica.of(topicName, 2, 1))
          .throttle(TopicPartitionReplica.of(topicName, 2, 2))
          .ingress(DataRate.MiB.of(500).perSecond())
          .egress(DataRate.MiB.of(500).perSecond())
          .apply();
      Utils.sleep(Duration.ofSeconds(1));

      System.out.println(handler.get(Optional.empty(), Map.of()).json());
    }
  }
}
