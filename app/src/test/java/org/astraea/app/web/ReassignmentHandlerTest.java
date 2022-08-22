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
import java.util.Set;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.common.Utils;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReassignmentHandlerTest extends RequireBrokerCluster {

  @Test
  void testMigrateToAnotherBroker() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ReassignmentHandler(admin);
      admin.creator().topic(topicName).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(3));

      var currentBroker =
          admin.replicas(Set.of(topicName)).get(TopicPartition.of(topicName, 0)).get(0).broker();
      var nextBroker = brokerIds().stream().filter(i -> i != currentBroker).findAny().get();

      var body =
          String.format(
              "{\"%s\": [{\"%s\": \"%s\",\"%s\": \"%s\",\"%s\": [%s]}]}",
              ReassignmentHandler.PLANS_KEY,
              ReassignmentHandler.TOPIC_KEY,
              topicName,
              ReassignmentHandler.PARTITION_KEY,
              "0",
              ReassignmentHandler.TO_KEY,
              nextBroker);

      Assertions.assertEquals(Response.ACCEPT, handler.post(PostRequest.of(body)));

      Utils.sleep(Duration.ofSeconds(2));
      var reassignments = handler.get(Optional.of(topicName), Map.of());
      // the reassignment should be completed
      Assertions.assertEquals(0, reassignments.reassignments.size());

      Assertions.assertEquals(
          nextBroker,
          admin.replicas(Set.of(topicName)).get(TopicPartition.of(topicName, 0)).get(0).broker());
    }
  }

  @Test
  void testMigrateToAnotherPath() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ReassignmentHandler(admin);
      admin.creator().topic(topicName).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(3));

      var currentReplica =
          admin.replicas(Set.of(topicName)).get(TopicPartition.of(topicName, 0)).get(0);
      var currentBroker = currentReplica.broker();
      var currentPath = currentReplica.path();
      var nextPath =
          logFolders().get(currentBroker).stream()
              .filter(p -> !p.equals(currentPath))
              .findFirst()
              .get();

      var body =
          String.format(
              "{\"%s\": [{\"%s\": \"%s\", \"%s\": \"%s\" ,\"%s\": \"%s\",\"%s\": \"%s\"}]}",
              ReassignmentHandler.PLANS_KEY,
              ReassignmentHandler.TOPIC_KEY,
              topicName,
              ReassignmentHandler.PARTITION_KEY,
              "0",
              ReassignmentHandler.BROKER_KEY,
              currentBroker,
              ReassignmentHandler.TO_KEY,
              nextPath);

      Assertions.assertEquals(Response.ACCEPT, handler.post(PostRequest.of(body)));

      Utils.sleep(Duration.ofSeconds(2));
      var reassignments = handler.get(Optional.of(topicName), Map.of());
      // the reassignment should be completed
      Assertions.assertEquals(0, reassignments.reassignments.size());

      Assertions.assertEquals(
          nextPath,
          admin.replicas(Set.of(topicName)).get(TopicPartition.of(topicName, 0)).get(0).path());
    }
  }

  @Test
  void testBadRequest() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new ReassignmentHandler(admin);
      admin.creator().topic(topicName).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(3));
      var body = "{\"plans\": []}";

      Assertions.assertEquals(Response.BAD_REQUEST, handler.post(PostRequest.of(body)));
    }
  }
}
