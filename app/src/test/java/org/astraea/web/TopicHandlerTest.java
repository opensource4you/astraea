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
      Assertions.assertEquals(
          1, response.topics.stream().filter(t -> t.name.equals(topicName)).count());
      Assertions.assertNotEquals(
          0,
          response.topics.stream()
              .filter(t -> t.name.equals(topicName))
              .findFirst()
              .get()
              .configs
              .size());
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

  @Test
  void testCreateTopic() {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var topicInfo =
          Assertions.assertInstanceOf(
              TopicHandler.TopicInfo.class,
              handler.post(PostRequest.of(Map.of(TopicHandler.TOPIC_NAME_KEY, topicName))));
      Assertions.assertEquals(topicName, topicInfo.name);
    }
  }

  @Test
  void testCreateTopicWithReplicas() throws InterruptedException {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var topicInfo =
          Assertions.assertInstanceOf(
              TopicHandler.TopicInfo.class,
              handler.post(
                  PostRequest.of(
                      Map.of(
                          TopicHandler.TOPIC_NAME_KEY,
                          topicName,
                          TopicHandler.NUMBER_OF_PARTITIONS_KEY,
                          "2",
                          TopicHandler.NUMBER_OF_REPLICAS_KEY,
                          "2",
                          "segment.ms",
                          "3000"))));
      Assertions.assertEquals(topicName, topicInfo.name);

      // the topic creation is not synced, so we have to wait the creation.
      if (topicInfo.partitions.isEmpty()) {
        TimeUnit.SECONDS.sleep(2);
        var result = admin.replicas(Set.of(topicName));
        Assertions.assertEquals(2, result.size());
        result.values().forEach(replicas -> Assertions.assertEquals(2, replicas.size()));
      } else {
        Assertions.assertEquals(2, topicInfo.partitions.size());
        Assertions.assertEquals(2, topicInfo.partitions.iterator().next().replicas.size());
      }
      Assertions.assertEquals(
          "3000", admin.topics(Set.of(topicName)).get(topicName).value("segment.ms").get());
    }
  }

  @Test
  void testRemainingConfigs() {
    Assertions.assertEquals(
        0,
        TopicHandler.remainingConfigs(
                PostRequest.of(
                    Map.of(
                        TopicHandler.TOPIC_NAME_KEY,
                        "abc",
                        TopicHandler.NUMBER_OF_PARTITIONS_KEY,
                        "2",
                        TopicHandler.NUMBER_OF_REPLICAS_KEY,
                        "2")))
            .size());

    Assertions.assertEquals(
        1,
        TopicHandler.remainingConfigs(
                PostRequest.of(Map.of(TopicHandler.TOPIC_NAME_KEY, "abc", "key", "value")))
            .size());
  }
}
