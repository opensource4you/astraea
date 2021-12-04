package org.astraea.topic.cost;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.astraea.producer.Producer;
import org.astraea.producer.Serializer;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PartitionScoreTest extends RequireBrokerCluster {
  static TopicAdmin admin;

  @BeforeAll
  static void setup() throws ExecutionException, InterruptedException {
    admin = TopicAdmin.of(bootstrapServers());
    Map<Integer, String> topicName = new HashMap<>();
    topicName.put(0, "testPartitionScore0");
    topicName.put(1, "testPartitionScore1");
    topicName.put(2, "testPartitionScore2");
    try (var admin = TopicAdmin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topicName.get(0))
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .create();
      admin
          .creator()
          .topic(topicName.get(1))
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .create();
      admin
          .creator()
          .topic(topicName.get(2))
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .create();
      // wait for topic
      TimeUnit.SECONDS.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    var producer =
        Producer.builder().brokers(bootstrapServers()).keySerializer(Serializer.STRING).build();
    int size = 10000;
    for (int t = 0; t <= 2; t++) {
      for (int p = 0; p <= 3; p++) {
        producer
            .sender()
            .topic(topicName.get(t))
            .partition(p)
            .value(new byte[size])
            .run()
            .toCompletableFuture()
            .get();
      }
      size += 10000;
    }
    producer.close();
  }

  @Test
  void testGetScore() throws ExecutionException, InterruptedException {
    PartitionScore partitionScore = new PartitionScore(bootstrapServers());
    assertEquals(3, partitionScore.score.size());
    assertEquals(
        3 * 4,
        partitionScore.score.get(0).size()
            + partitionScore.score.get(1).size()
            + partitionScore.score.get(2).size());
  }
}
