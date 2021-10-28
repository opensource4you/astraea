package org.astraea.moveCost;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.*;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PartitionScoreTest extends RequireBrokerCluster {

  @BeforeAll
  static void setup() {
    var topicName_0 = "testPartitionScore0";
    var topicName_1 = "testPartitionScore1";
    var topicName_2 = "testPartitionScore2";
    try (var admin = TopicAdmin.of(bootstrapServers())) {
      admin.createTopic(topicName_0, 4, (short) 1);
      admin.createTopic(topicName_1, 4, (short) 1);
      admin.createTopic(topicName_2, 4, (short) 1);
      // wait for topic
      TimeUnit.SECONDS.sleep(5);
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", bootstrapServers());
    properties.setProperty(
        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.setProperty(
        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    var kafkaProducer = new KafkaProducer<String, String>(properties);
    kafkaProducer.send(
        new ProducerRecord<>(
            topicName_0,
            Integer.toString(0),
            new Random()
                .ints('a', 'z')
                .limit(100)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString()));
    kafkaProducer.send(
        new ProducerRecord<>(
            topicName_1,
            Integer.toString(0),
            new Random()
                .ints('a', 'z')
                .limit(120)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString()));
    kafkaProducer.close();
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
