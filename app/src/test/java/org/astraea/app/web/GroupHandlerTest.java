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
import java.util.concurrent.TimeUnit;
import org.astraea.app.admin.Admin;
import org.astraea.app.common.Utils;
import org.astraea.app.consumer.Consumer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GroupHandlerTest extends RequireBrokerCluster {

  @Test
  void testListGroups() throws InterruptedException {
    var topicName = Utils.randomString(10);
    var groupId = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).create();
      TimeUnit.SECONDS.sleep(3);

      try (var consumer =
          Consumer.builder()
              .groupId(groupId)
              .topics(Set.of(topicName))
              .bootstrapServers(bootstrapServers())
              .build()) {
        Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
        var handler = new GroupHandler(admin);
        var response =
            Assertions.assertInstanceOf(
                GroupHandler.Groups.class, handler.get(Optional.empty(), Map.of()));
        Assertions.assertEquals(1, response.groups.size());
        Assertions.assertEquals(groupId, response.groups.iterator().next().groupId);
        Assertions.assertEquals(1, response.groups.iterator().next().members.size());
        response
            .groups
            .iterator()
            .next()
            .members
            .forEach(m -> Assertions.assertNull(m.groupInstanceId));
      }
    }
  }

  @Test
  void testQueryNonexistentGroup() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new GroupHandler(admin);
      Assertions.assertThrows(
          NoSuchElementException.class, () -> handler.get(Optional.of("unknown"), Map.of()));
    }
  }

  @Test
  void testQuerySingleGroup() {
    var topicName = Utils.randomString(10);
    var groupId = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new GroupHandler(admin);

      try (var consumer =
          Consumer.builder()
              .groupId(groupId)
              .topics(Set.of(topicName))
              .bootstrapServers(bootstrapServers())
              .build()) {
        Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
        var group =
            Assertions.assertInstanceOf(
                GroupHandler.Group.class, handler.get(Optional.of(groupId), Map.of()));
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
          Consumer.builder()
              .groupId(groupId)
              .topics(Set.of(topicName))
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
}
