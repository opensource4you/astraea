package org.astraea.topic;

import java.io.IOException;
import java.util.Set;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopicAdminTest extends RequireBrokerCluster {

  @Test
  void testPartitions() throws IOException {
    var topicName = "testPartitions";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin.createTopic(topicName, 3);
      Assertions.assertTrue(topicAdmin.topics().contains(topicName));
      var partitions = topicAdmin.replicas(Set.of(topicName));
      Assertions.assertEquals(3, partitions.size());
    }
  }

  @Test
  void testGroups() throws IOException {
    var topicName = "testGroups";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin.createTopic(topicName, 3);
      Assertions.assertTrue(topicAdmin.topics().contains(topicName));
      var groups = topicAdmin.groups(Set.of(topicName));
      Assertions.assertEquals(0, groups.size());
    }
  }

  @Test
  void testOffsets() throws IOException {
    var topicName = "testOffsets";
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin.createTopic(topicName, 3);
      var offsets = topicAdmin.offsets(Set.of(topicName));
      Assertions.assertEquals(3, offsets.size());
      offsets
          .values()
          .forEach(
              offset -> {
                Assertions.assertEquals(0, offset.earliest);
                Assertions.assertEquals(0, offset.latest);
              });
    }
  }
}
