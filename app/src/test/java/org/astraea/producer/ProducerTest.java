package org.astraea.producer;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.astraea.consumer.Builder;
import org.astraea.consumer.Consumer;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProducerTest extends RequireBrokerCluster {

  @Test
  void testSender() throws ExecutionException, InterruptedException {
    var topicName = "testSender-" + System.currentTimeMillis();
    var key = "key";
    var timestamp = System.currentTimeMillis() + 10;
    try (var producer =
        Producer.builder()
            .brokers(bootstrapServers())
            .keySerializer(new StringSerializer())
            .build()) {
      var metadata =
          producer
              .sender()
              .topic(topicName)
              .key(key)
              .timestamp(timestamp)
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
            .keyDeserializer(new StringDeserializer())
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(10));
      Assertions.assertEquals(1, records.size());
      var record = records.iterator().next();
      Assertions.assertEquals(topicName, record.topic());
      Assertions.assertEquals("key", record.key());
    }
  }
}
