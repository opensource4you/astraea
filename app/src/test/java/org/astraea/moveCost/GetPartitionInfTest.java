package org.astraea.moveCost;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GetPartitionInfTest extends RequireBrokerCluster {
  static AdminClient client;

  @BeforeAll
  static void setup() {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers());
    client = AdminClient.create(props);
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
  void getSize() throws ExecutionException, InterruptedException {

    var brokerPartitionSize = GetPartitionInf.getSize(client);
    assertEquals(3, brokerPartitionSize.size());
    assertEquals(
        3 * 4,
        brokerPartitionSize.get(0).size()
            + brokerPartitionSize.get(1).size()
            + brokerPartitionSize.get(2).size());
  }

  @Test
  void getRetention_ms() throws ExecutionException, InterruptedException {
    var brokerPartitionRetention_ms = GetPartitionInf.getRetention_ms(client);
    assertEquals(3, brokerPartitionRetention_ms.size());
  }
}
