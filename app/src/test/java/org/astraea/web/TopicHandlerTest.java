package org.astraea.web;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.astraea.Utils;
import org.astraea.admin.Admin;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopicHandlerTest extends RequireBrokerCluster {

  @Test
  void testListTopics() throws InterruptedException {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).create();
      TimeUnit.SECONDS.sleep(3);
      var handler = new TopicHandler(admin);
      var response =
          Assertions.assertInstanceOf(
              TopicHandler.Topics.class, handler.get(Optional.empty(), Map.of()));
      Assertions.assertEquals(1, response.topics.size());
      Assertions.assertEquals(topicName, response.topics.iterator().next().name);
      Assertions.assertNotEquals(0, response.topics.iterator().next().configs.size());
    }
  }

  @Test
  void testQueryNonexistentTopic() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      Assertions.assertThrows(
          NoSuchElementException.class, () -> handler.get(Optional.of("unknown"), Map.of()));
    }
  }

  @Test
  void testQuerySingleTopic() throws InterruptedException {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).create();
      TimeUnit.SECONDS.sleep(3);
      var handler = new TopicHandler(admin);
      var topicInfo =
          Assertions.assertInstanceOf(
              TopicHandler.TopicInfo.class, handler.get(Optional.of(topicName), Map.of()));
      Assertions.assertEquals(topicName, topicInfo.name);
      Assertions.assertNotEquals(0, topicInfo.configs.size());
    }
  }

  @Test
  void testTopics() throws InterruptedException {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).create();
      TimeUnit.SECONDS.sleep(3);
      var handler = new TopicHandler(admin);
      Assertions.assertEquals(Set.of(topicName), handler.topicNames(Optional.of(topicName)));
      Assertions.assertThrows(
          NoSuchElementException.class,
          () -> handler.topicNames(Optional.of(Utils.randomString(10))));
      Assertions.assertTrue(handler.topicNames(Optional.empty()).contains(topicName));
    }
  }
}
