package org.astraea.partitioner.nodeLoadMetric;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ser.impl.StringArraySerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.consumer.Consumer;
import org.astraea.consumer.Deserializer;
import org.astraea.consumer.Header;
import org.astraea.partitioner.partitionerFactory.DependencyClient;
import org.astraea.partitioner.partitionerFactory.SmoothWeightPartitioner;
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
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SmoothWeightPartitioner.class.getName());
    props.put("producerID",1);
    props.put("jmx_servers", jmxServiceURL().getHost() + ":" + jmxServiceURL().getPort());
    return props;
  }

  @Test
  public void testPartitioner() {
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
            .fromBeginning()
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

  @Test
  public void testDependencyPartitioner(){
    admin.creator().topic(topicName).numberOfPartitions(10).create();
    var dependencyClient = new DependencyClient();
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());
    var props = initProConfig().entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
    try (var producer =
                 Producer.builder()
                         .configs(props)
                         .build()) {


      dependencyClient.initializeDependency(props);
      dependencyClient.beginDependency(props);
      int targetPartition = 0;
      for (int i = 0; i < 20; i++) {
        var metadata =
                producer
                        .sender()
                        .topic(topicName)
                        .timestamp(timestamp)
                        .headers(List.of(header))
                        .run()
                        .toCompletableFuture()
                        .get();
        if (i >0) Assertions.assertEquals(targetPartition,metadata.partition());
        targetPartition = metadata.partition();
        Assertions.assertEquals(topicName, metadata.topic());
        Assertions.assertEquals(timestamp, metadata.timestamp());
      }
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
    dependencyClient.finishDependency(props);


    try (var consumer =
                 Consumer.builder()
                         .brokers(bootstrapServers())
                         .fromBeginning()
                         .topics(Set.of(topicName))
                         .keyDeserializer(Deserializer.STRING)
                         .build()) {
      var records = consumer.poll(Duration.ofSeconds(10));
      Assertions.assertEquals(20, records.size());
      var record = records.iterator().next();
      Assertions.assertEquals(topicName, record.topic());
      Assertions.assertEquals(1, record.headers().size());
      var actualHeader = record.headers().iterator().next();
      Assertions.assertEquals(header.key(), actualHeader.key());
      Assertions.assertArrayEquals(header.value(), actualHeader.value());
    }
  }
}
