package org.astraea.partitioner.nodeLoadMetric;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.consumer.Builder;
import org.astraea.consumer.Consumer;
import org.astraea.consumer.Deserializer;
import org.astraea.consumer.Header;
import org.astraea.partitioner.partitionerFactory.LinkPartitioner;
import org.astraea.producer.Producer;
import org.astraea.producer.Serializer;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionerTest extends RequireBrokerCluster {
  public final String brokerList = bootstrapServers();
  TopicAdmin admin = TopicAdmin.of(bootstrapServers());
  public final String topicName = "address";

  public Properties initProConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, LinkPartitioner.class.getName());
    props.put("jmx_servers", jmxServiceURL() + "@0");
    return props;
  }

  @Test
  public void testPartitioner() {
    admin.createTopic(topicName, 10);

    var key = "tainan";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());
    try (var producer =
        Producer.builder()
            .keySerializer(Serializer.STRING)
            .configs(
                initProConfig().entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)))
            .build()) {
      for (int i = 0; i < 20; i++) {
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
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }

    try (var consumer =
        Consumer.builder()
            .brokers(bootstrapServers())
            .offsetPolicy(Builder.OffsetPolicy.EARLIEST)
            .topics(Set.of(topicName))
            .keyDeserializer(Deserializer.STRING)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(10));
      Assertions.assertEquals(20, records.size());
      var record = records.iterator().next();
      Assertions.assertEquals(topicName, record.topic());
      Assertions.assertEquals("tainan", record.key());
      Assertions.assertEquals(1, record.headers().size());
      var actualHeader = record.headers().iterator().next();
      Assertions.assertEquals(header.key(), actualHeader.key());
      Assertions.assertArrayEquals(header.value(), actualHeader.value());
    }
  }
}
