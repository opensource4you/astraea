package org.astraea.topic;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ReplicaFolderTest extends RequireBrokerCluster {
  @Test
  void testVerify() throws IOException, InterruptedException {
    test(true);
  }

  @Test
  void testExecute() throws IOException, InterruptedException {
    test(false);
  }

  void test(boolean verify) {
    var topicName = "ReplicaFolderTest-" + verify;
    try (var topicAdmin = TopicAdmin.of(bootstrapServers())) {
      topicAdmin
          .creator()
          .topic(topicName)
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .create();
      // wait for topic creation
      TimeUnit.SECONDS.sleep(5);

      var partitionReplicas = topicAdmin.replicas(Set.of(topicName));
      Assertions.assertEquals(4, partitionReplicas.size());
      var replicas =
          partitionReplicas.get(new TopicPartition(topicName, 0)).stream()
              .filter(Replica::isCurrent)
              .collect(Collectors.toList());
      Assertions.assertEquals(1, replicas.size());
      var badBroker = replicas.get(0).broker();
      var argument = new ReplicaFolder.Argument();
      argument.brokers = bootstrapServers();
      argument.topics = Set.of(topicName);
      argument.partitions = Set.of("0");
      argument.path = Set.of("");
      argument.verify = verify;

    } catch (Exception e) {
    }
  }
}
