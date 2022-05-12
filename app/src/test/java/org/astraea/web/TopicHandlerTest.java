package org.astraea.web;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.astraea.Utils;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopicHandlerTest extends RequireBrokerCluster {

  @Test
  void testListTopics() throws InterruptedException {
    var topicName = Utils.randomString(10);
    try (TopicAdmin admin = TopicAdmin.of(bootstrapServers())) {
      admin.creator().topic(topicName).create();
      TimeUnit.SECONDS.sleep(3);
      var handler = new TopicHandler(admin);
      var response =
          Assertions.assertInstanceOf(
              TopicHandler.Topics.class, handler.response(Optional.empty(), Map.of()));
      Assertions.assertEquals(1, response.topics.size());
      Assertions.assertEquals(topicName, response.topics.iterator().next().name);
      Assertions.assertNotEquals(0, response.topics.iterator().next().configs.size());
    }
  }

  @Test
  void testQueryNonexistentTopic() {
    try (TopicAdmin admin = TopicAdmin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var exception =
          Assertions.assertThrows(
              NoSuchElementException.class,
              () -> handler.response(Optional.of("unknown"), Map.of()));
      Assertions.assertTrue(exception.getMessage().contains("unknown"));
    }
  }

  @Test
  void testQuerySingleTopic() throws InterruptedException {
    var topicName = Utils.randomString(10);
    try (TopicAdmin admin = TopicAdmin.of(bootstrapServers())) {
      admin.creator().topic(topicName).create();
      TimeUnit.SECONDS.sleep(3);
      var handler = new TopicHandler(admin);
      var topicInfo =
          Assertions.assertInstanceOf(
              TopicHandler.TopicInfo.class, handler.response(Optional.of(topicName), Map.of()));
      Assertions.assertEquals(topicName, topicInfo.name);
      Assertions.assertNotEquals(0, topicInfo.configs.size());
    }
  }
}
