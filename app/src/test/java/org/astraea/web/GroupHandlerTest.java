package org.astraea.web;

import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.astraea.admin.Admin;
import org.astraea.common.Utils;
import org.astraea.consumer.Consumer;
import org.astraea.service.RequireBrokerCluster;
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
