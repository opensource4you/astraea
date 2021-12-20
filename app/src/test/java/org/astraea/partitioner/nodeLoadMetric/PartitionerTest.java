package org.astraea.partitioner.nodeLoadMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.HashMap;
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
import org.astraea.partitioner.smoothPartitioner.SmoothWeightPartitioner;
import org.astraea.producer.Producer;
import org.astraea.producer.Serializer;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionerTest extends RequireBrokerCluster {
  private final String brokerList = bootstrapServers();
  TopicAdmin admin = TopicAdmin.of(bootstrapServers());
  private final String topicName = "address";

  private Properties initProConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SmoothWeightPartitioner.class.getName());
    props.put("producerID", 1);
    props.put("jmx_servers", jmxServiceURL().getHost() + ":" + jmxServiceURL().getPort());
    return props;
  }

  @Test
  void testPartitioner() {
    admin.creator().topic(topicName).numberOfPartitions(10).create();
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
      var i = 0;
      while (i < 20) {
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
      assertEquals(20, records.size());
      var record = records.iterator().next();
      assertEquals(topicName, record.topic());
      assertEquals("tainan", record.key());
      assertEquals(1, record.headers().size());
      var actualHeader = record.headers().iterator().next();
      assertEquals(header.key(), actualHeader.key());
      Assertions.assertArrayEquals(header.value(), actualHeader.value());
    }
  }

  @Test
  void testSetBrokerHashMap() {
    var poissonMap = new HashMap<Integer, Double>();
    poissonMap.put(0, 0.5);
    poissonMap.put(1, 0.8);
    poissonMap.put(2, 0.3);

    var smoothWeightPartitioner = new SmoothWeightPartitioner();

    smoothWeightPartitioner.brokerHashMap(poissonMap);

    assertEquals(smoothWeightPartitioner.brokerHashMap().get(0)[0], 10);
    assertEquals(smoothWeightPartitioner.brokerHashMap().get(1)[0], 3);

    smoothWeightPartitioner.brokerHashMapValue(0, 8);
    smoothWeightPartitioner.brokerHashMap(poissonMap);
    assertEquals(smoothWeightPartitioner.brokerHashMap().get(0)[1], 8);
  }
}
