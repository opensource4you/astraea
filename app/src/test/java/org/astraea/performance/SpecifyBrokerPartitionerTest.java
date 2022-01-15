package org.astraea.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.consumer.Consumer;
import org.astraea.consumer.Deserializer;
import org.astraea.consumer.Header;
import org.astraea.producer.Producer;
import org.astraea.producer.Serializer;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SpecifyBrokerPartitionerTest extends RequireBrokerCluster {
  private final String brokerList = bootstrapServers();
  TopicAdmin admin = TopicAdmin.of(bootstrapServers());
  private final String topicName = "address";

  private Properties initProConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SpecifyBrokerPartitioner.class.getName());
    props.put("producerID", 1);
    props.put("specify_broker", bootstrapServers().split(",")[0]);
    return props;
  }

  @Test
  void testPartitioner() {
    var props = initProConfig();
    admin.creator().topic(topicName).numberOfPartitions(10).create();
    var key = "tainan";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());
    try (var producer =
        Producer.builder()
            .keySerializer(Serializer.STRING)
            .configs(
                props.entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)))
            .build()) {
      var i = 0;
      var lastPartition = -1;
      while (i < 100) {
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
        assertEquals(topicName, metadata.topic());
        assertEquals(timestamp, metadata.timestamp());
        if (lastPartition == -1) {
          lastPartition = metadata.partition();
        } else {
          var partition = metadata.partition();
          Assertions.assertEquals((lastPartition - partition) % 3, 0);
        }
        i++;
      }
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }

    try (var consumer =
        Consumer.builder()
            .brokers(bootstrapServers())
            .fromBeginning()
            .topics(Set.of(topicName))
            .keyDeserializer(Deserializer.STRING)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(10));
      assertEquals(100, records.size());
      var record = records.iterator().next();
      assertEquals(topicName, record.topic());
      assertEquals("tainan", record.key());
      assertEquals(1, record.headers().size());
      var actualHeader = record.headers().iterator().next();
      assertEquals(header.key(), actualHeader.key());
      Assertions.assertArrayEquals(header.value(), actualHeader.value());
    }
  }
}
