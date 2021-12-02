package org.astraea.producer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.astraea.consumer.Builder;
import org.astraea.consumer.Consumer;
import org.astraea.consumer.Deserializer;
import org.astraea.consumer.Header;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProducerTest extends RequireBrokerCluster {

  @Test
  void testSender() throws ExecutionException, InterruptedException {
    var topicName = "testSender-" + System.currentTimeMillis();
    var key = "key";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());
    try (var producer =
        Producer.builder().brokers(bootstrapServers()).keySerializer(Serializer.STRING).build()) {
      var metadata =
          producer
              .sender()
              .topic(topicName)
              .key(key)
              .timestamp(timestamp)
              .headers(List.of(header))
              .run()
              .toCompletableFuture()
              .get();
      Assertions.assertEquals(topicName, metadata.topic());
      Assertions.assertEquals(timestamp, metadata.timestamp());
    }

    try (var consumer =
        Consumer.builder()
            .brokers(bootstrapServers())
            .offsetPolicy(Builder.OffsetPolicy.EARLIEST)
            .topics(Set.of(topicName))
            .keyDeserializer(Deserializer.STRING)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(10));
      Assertions.assertEquals(1, records.size());
      var record = records.iterator().next();
      Assertions.assertEquals(topicName, record.topic());
      Assertions.assertEquals("key", record.key());
      Assertions.assertEquals(1, record.headers().size());
      var actualHeader = record.headers().iterator().next();
      Assertions.assertEquals(header.key(), actualHeader.key());
      Assertions.assertArrayEquals(header.value(), actualHeader.value());
    }
  }

  @Test
  void testTransaction() {
    var topicName = "testTransaction-" + System.currentTimeMillis();
    Map<String, Object> producerProp = Map.of(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "1");
    try (var producer =
        Producer.builder()
            .configs(producerProp)
            .brokers(bootstrapServers())
            .keySerializer(Serializer.STRING)
            .build()) {
      producer.beginTransaction();
      producer.sender().topic(topicName).run();
      Map<String, Object> consumerProp =
          Map.of(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
      try (var consumer =
          Consumer.builder()
              .brokers(bootstrapServers())
              .offsetPolicy(Builder.OffsetPolicy.EARLIEST)
              .topics(Set.of(topicName))
              .keyDeserializer(Deserializer.STRING)
              .configs(consumerProp)
              .build()) {
        var records = consumer.poll(Duration.ofSeconds(5));
        Assertions.assertEquals(0, records.size());

        producer.commitTransaction();

        records = consumer.poll(Duration.ofSeconds(10));
        Assertions.assertEquals(1, records.size());
      }
    }
  }
}
