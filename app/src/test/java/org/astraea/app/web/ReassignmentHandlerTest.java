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

import static org.astraea.app.web.ReassignmentHandler.progressInPercentage;
import static org.astraea.app.web.ReassignmentHandler.toReassignment;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Reassignment;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.it.RequireBrokerCluster;
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
          admin
              .replicas(Set.of(topicName))
              .get(TopicPartition.of(topicName, 0))
              .get(0)
              .nodeInfo()
              .id();
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

      Assertions.assertEquals(
          Response.ACCEPT, handler.post(Channel.ofRequest(PostRequest.of(body))));

      Utils.sleep(Duration.ofSeconds(2));
      var reassignments = handler.get(Channel.EMPTY);
      // the reassignment should be completed
      Assertions.assertEquals(0, reassignments.reassignments.size());

      Assertions.assertEquals(
          nextBroker,
          admin
              .replicas(Set.of(topicName))
              .get(TopicPartition.of(topicName, 0))
              .get(0)
              .nodeInfo()
              .id());
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
      var currentBroker = currentReplica.nodeInfo().id();
      var currentPath = currentReplica.dataFolder();
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

      Assertions.assertEquals(
          Response.ACCEPT, handler.post(Channel.ofRequest(PostRequest.of(body))));

      Utils.sleep(Duration.ofSeconds(2));
      var reassignments = handler.get(Channel.ofTarget(topicName));
      // the reassignment should be completed
      Assertions.assertEquals(0, reassignments.reassignments.size());

      Assertions.assertEquals(
          nextPath,
          admin
              .replicas(Set.of(topicName))
              .get(TopicPartition.of(topicName, 0))
              .get(0)
              .dataFolder());
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

      Assertions.assertEquals(
          Response.BAD_REQUEST, handler.post(Channel.ofRequest(PostRequest.of(body))));
    }
  }

  @Test
  void testToReassignment() {
    var reassignment =
        toReassignment(
            TopicPartition.of("test", 0),
            List.of(
                new Reassignment.Location(1001, "/tmp/dir1"),
                new Reassignment.Location(1002, "/tmp/dir1")),
            List.of(
                new Reassignment.Location(1001, "/tmp/dir2"),
                new Reassignment.Location(1003, "/tmp/dir3")),
            List.of(
                fakeReplica(TopicPartition.of("test", 0), 1001, 200, "/tmp/dir1"),
                fakeReplica(TopicPartition.of("test", 0), 1002, 200, "/tmp/dir1"),
                fakeReplica(TopicPartition.of("test", 0), 1001, 20, "/tmp/dir2"),
                fakeReplica(TopicPartition.of("test", 0), 1003, 30, "/tmp/dir3")));

    Assertions.assertEquals(reassignment.topicName, "test");
    Assertions.assertEquals(reassignment.partition, 0);

    Assertions.assertEquals(reassignment.from.size(), 2);
    var fromIterator = reassignment.from.iterator();
    var from = fromIterator.next();
    Assertions.assertEquals(from.broker, 1001);
    Assertions.assertEquals(from.size, 200);
    Assertions.assertEquals(from.path, "/tmp/dir1");
    from = fromIterator.next();
    Assertions.assertEquals(from.broker, 1002);
    Assertions.assertEquals(from.size, 200);
    Assertions.assertEquals(from.path, "/tmp/dir1");

    Assertions.assertEquals(reassignment.to.size(), 2);
    var toIterator = reassignment.to.iterator();
    var to = toIterator.next();
    Assertions.assertEquals(to.broker, 1001);
    Assertions.assertEquals(to.size, 20);
    Assertions.assertEquals(to.path, "/tmp/dir2");
    to = toIterator.next();
    Assertions.assertEquals(to.broker, 1003);
    Assertions.assertEquals(to.size, 30);
    Assertions.assertEquals(to.path, "/tmp/dir3");

    Assertions.assertEquals(reassignment.progress, "12.50%");
  }

  private static Replica fakeReplica(
      TopicPartition topicPartition, int brokerId, long size, String dataFolder) {
    return Replica.of(
        topicPartition.topic(),
        topicPartition.partition(),
        NodeInfo.of(brokerId, "", 12345),
        0,
        size,
        true,
        true,
        true,
        true,
        true,
        dataFolder);
  }

  @Test
  void testProgressInPercentage() {
    Assertions.assertEquals(progressInPercentage(-1.1), "0.00%");
    Assertions.assertEquals(progressInPercentage(11.11), "100.00%");
    Assertions.assertEquals(progressInPercentage(0.12345), "12.35%");
  }
}
