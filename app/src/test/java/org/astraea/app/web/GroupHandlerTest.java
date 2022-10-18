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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GroupHandlerTest extends RequireBrokerCluster {

  @Test
  void testListGroups() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    var groupId = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).create();
      Utils.sleep(Duration.ofSeconds(3));

      try (var consumer =
          Consumer.forTopics(Set.of(topicName))
              .config(ConsumerConfigs.GROUP_ID_CONFIG, groupId)
              .bootstrapServers(bootstrapServers())
              .build()) {
        Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
        var handler = new GroupHandler(admin);
        var response =
            Assertions.assertInstanceOf(
                GroupHandler.Groups.class, handler.get(Channel.EMPTY).toCompletableFuture().get());
        var group = response.groups.stream().filter(g -> g.groupId.equals(groupId)).findAny().get();
        Assertions.assertEquals(1, group.members.size());
        group.members.forEach(m -> Assertions.assertTrue(m.groupInstanceId.isEmpty()));
      }
    }
  }

  @Test
  void testQueryNonexistentGroup() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new GroupHandler(admin);
      Assertions.assertThrows(
          NoSuchElementException.class,
          () -> handler.get(Channel.ofTarget("unknown")).toCompletableFuture().get());
    }
  }

  @Test
  void testQuerySingleGroup() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    var groupId = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new GroupHandler(admin);

      try (var consumer =
          Consumer.forTopics(Set.of(topicName))
              .config(ConsumerConfigs.GROUP_ID_CONFIG, groupId)
              .bootstrapServers(bootstrapServers())
              .build()) {
        Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
        var group =
            Assertions.assertInstanceOf(
                GroupHandler.Group.class,
                handler.get(Channel.ofTarget(groupId)).toCompletableFuture().get());
        Assertions.assertEquals(groupId, group.groupId);
        Assertions.assertEquals(1, group.members.size());
      }
    }
  }

  @Test
  void testGroups() {
    var topicName = Utils.randomString(10);
    var groupId = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new GroupHandler(admin);

      try (var consumer =
          Consumer.forTopics(Set.of(topicName))
              .config(ConsumerConfigs.GROUP_ID_CONFIG, groupId)
              .bootstrapServers(bootstrapServers())
              .build()) {
        Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
        Assertions.assertEquals(Set.of(groupId), handler.groupIds(Optional.of(groupId)));
        Assertions.assertThrows(
            NoSuchElementException.class,
            () -> handler.groupIds(Optional.of(Utils.randomString(10))));
        Assertions.assertTrue(handler.groupIds(Optional.empty()).contains(groupId));
      }
    }
  }

  @Test
  void testSpecifyTopic() throws ExecutionException, InterruptedException {
    var topicName0 = Utils.randomString(10);
    var topicName1 = Utils.randomString(10);
    var groupId0 = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new GroupHandler(admin);

      try (var consumer0 =
              Consumer.forTopics(Set.of(topicName0))
                  .config(ConsumerConfigs.GROUP_ID_CONFIG, groupId0)
                  .bootstrapServers(bootstrapServers())
                  .config(
                      ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                      ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
                  .build();
          var consumer1 =
              Consumer.forTopics(Set.of(topicName1)).bootstrapServers(bootstrapServers()).build();
          var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
        producer.sender().topic(topicName0).key(new byte[2]).run();
        producer.flush();
        Assertions.assertEquals(1, consumer0.poll(1, Duration.ofSeconds(10)).size());
        Assertions.assertEquals(0, consumer1.poll(Duration.ofSeconds(2)).size());

        Utils.sleep(Duration.ofSeconds(3));

        // query for only topicName0 so only group of consumer0 can be returned.
        consumer0.commitOffsets(Duration.ofSeconds(3));
        var response =
            Assertions.assertInstanceOf(
                GroupHandler.Groups.class,
                handler
                    .get(Channel.ofQueries(Map.of(GroupHandler.TOPIC_KEY, topicName0)))
                    .toCompletableFuture()
                    .get());
        Assertions.assertEquals(1, response.groups.size());
        Assertions.assertEquals(groupId0, response.groups.get(0).groupId);

        // query all
        var all =
            Assertions.assertInstanceOf(
                GroupHandler.Groups.class, handler.get(Channel.EMPTY).toCompletableFuture().get());
        Assertions.assertNotEquals(1, all.groups.size());
      }
    }
  }

  @Test
  void testDeleteMembers() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new GroupHandler(admin);

      // test 0: delete all members
      try (var consumer =
          Consumer.forTopics(Set.of(topicName)).bootstrapServers(bootstrapServers()).build()) {
        Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
        Assertions.assertEquals(
            1,
            admin.consumerGroups(Set.of(consumer.groupId())).stream()
                .filter(g -> g.groupId().equals(consumer.groupId()))
                .findFirst()
                .get()
                .assignment()
                .size());

        handler.delete(Channel.ofTarget(consumer.groupId()));
        Assertions.assertEquals(
            0,
            admin.consumerGroups(Set.of(consumer.groupId())).stream()
                .filter(g -> g.groupId().equals(consumer.groupId()))
                .findFirst()
                .get()
                .assignment()
                .size());

        // idempotent test
        handler.delete(Channel.ofTarget(consumer.groupId()));
      }

      // test 1: delete static member
      try (var consumer =
          Consumer.forTopics(Set.of(topicName))
              .bootstrapServers(bootstrapServers())
              .config(ConsumerConfigs.GROUP_INSTANCE_ID_CONFIG, Utils.randomString(10))
              .build()) {
        Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
        Assertions.assertEquals(
            1,
            admin.consumerGroups(Set.of(consumer.groupId())).stream()
                .filter(g -> g.groupId().equals(consumer.groupId()))
                .findFirst()
                .get()
                .assignment()
                .size());

        handler.delete(
            Channel.ofQueries(
                consumer.groupId(),
                Map.of(GroupHandler.INSTANCE_KEY, consumer.groupInstanceId().get())));
        Assertions.assertEquals(
            0,
            admin.consumerGroups(Set.of(consumer.groupId())).stream()
                .filter(g -> g.groupId().equals(consumer.groupId()))
                .findFirst()
                .get()
                .assignment()
                .size());

        // idempotent test
        handler.delete(
            Channel.ofQueries(
                consumer.groupId(),
                Map.of(GroupHandler.INSTANCE_KEY, consumer.groupInstanceId().get())));
      }
    }
  }

  @Test
  void testDeleteGroup() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new GroupHandler(admin);

      var groupIds =
          IntStream.range(0, 3).mapToObj(x -> Utils.randomString(10)).collect(Collectors.toList());
      groupIds.forEach(
          groupId -> {
            try (var consumer =
                Consumer.forTopics(Set.of(topicName))
                    .bootstrapServers(bootstrapServers())
                    .config(ConsumerConfigs.GROUP_INSTANCE_ID_CONFIG, Utils.randomString(10))
                    .config(ConsumerConfigs.GROUP_ID_CONFIG, groupId)
                    .build()) {
              Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
            }
          });

      var currentGroupIds = admin.consumerGroupIds();
      Assertions.assertTrue(currentGroupIds.contains(groupIds.get(0)));
      Assertions.assertTrue(currentGroupIds.contains(groupIds.get(1)));
      Assertions.assertTrue(currentGroupIds.contains(groupIds.get(2)));

      handler.delete(Channel.ofTarget(groupIds.get(2)));
      Assertions.assertTrue(admin.consumerGroupIds().contains(groupIds.get(2)));

      handler.delete(Channel.ofQueries(groupIds.get(2), Map.of(GroupHandler.GROUP_KEY, "false")));
      Assertions.assertTrue(admin.consumerGroupIds().contains(groupIds.get(2)));

      handler.delete(Channel.ofQueries(groupIds.get(2), Map.of(GroupHandler.GROUP_KEY, "true")));
      Assertions.assertFalse(admin.consumerGroupIds().contains(groupIds.get(2)));

      var group1Members =
          admin.consumerGroups(Set.of(groupIds.get(1))).stream()
              .filter(g -> g.groupId().equals(groupIds.get(1)))
              .findFirst()
              .get()
              .assignment()
              .keySet();
      handler.delete(
          Channel.ofQueries(
              groupIds.get(1),
              Map.of(
                  GroupHandler.GROUP_KEY,
                  "true",
                  GroupHandler.INSTANCE_KEY,
                  group1Members.iterator().next().groupInstanceId().get())));
      Assertions.assertFalse(admin.consumerGroupIds().contains(groupIds.get(1)));
    }
  }
}
