package org.astraea.topic;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopicExplorerTest extends RequireBrokerCluster {

  @Test
  void testExecute() throws InterruptedException {
    var topicName = "testExecute";
    try (var admin = TopicAdmin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(1).numberOfReplicas((short) 1).create();
      // wait for topic
      TimeUnit.SECONDS.sleep(5);
      var result = TopicExplorer.execute(admin, Set.of(topicName));
      Assertions.assertEquals(1, result.size());
      Assertions.assertEquals(1, result.get(topicName).size());
      Assertions.assertEquals(topicName, result.get(topicName).get(0).topicPartition.topic());
      Assertions.assertEquals(0, result.get(topicName).get(0).topicPartition.partition());
      Assertions.assertEquals(0, result.get(topicName).get(0).earliestOffset);
      Assertions.assertEquals(0, result.get(topicName).get(0).latestOffset);
      Assertions.assertEquals(0, result.get(topicName).get(0).consumerGroups.size());
      Assertions.assertEquals(1, result.get(topicName).get(0).replicas.size());
    }
  }
}
