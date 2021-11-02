package org.astraea.moveCost;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.astraea.producer.Producer;
import org.astraea.producer.Serializer;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GetPartitionInfTest extends RequireBrokerCluster {
  static AdminClient client;

  @BeforeAll
  static void setup() throws ExecutionException, InterruptedException {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers());
    client = AdminClient.create(props);
    Map<Integer, String> topicName = new HashMap<>();
    topicName.put(0, "testPartitionScore0");
    topicName.put(1, "testPartitionScore1");
    topicName.put(2, "testPartitionScore2");

    try (var admin = TopicAdmin.of(bootstrapServers())) {
      admin.creator().topic(topicName.get(0)).numberOfPartitions(4).create();
      admin.creator().topic(topicName.get(1)).numberOfPartitions(4).create();
      admin.creator().topic(topicName.get(2)).numberOfPartitions(4).create();
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
