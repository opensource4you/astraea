package org.astraea.partitioner.smoothPartitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.concurrent.Executor;
import org.astraea.concurrent.State;
import org.astraea.concurrent.ThreadPool;
import org.astraea.consumer.Consumer;
import org.astraea.consumer.Deserializer;
import org.astraea.consumer.Header;
import org.astraea.producer.Producer;
import org.astraea.producer.Serializer;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SmoothWeightRoundRobinDispatchTest extends RequireBrokerCluster {
  private final String brokerList = bootstrapServers();
  TopicAdmin admin = TopicAdmin.of(bootstrapServers());

  private Properties initProConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(
        ProducerConfig.PARTITIONER_CLASS_CONFIG, SmoothWeightRoundRobinDispatcher.class.getName());
    props.put("producerID", 1);
    var file = new File(PartitionerTest.class.getResource("").getPath() + "PartitionerConfigTest");
    try {
      var fileWriter = new FileWriter(file);
      fileWriter.write("jmx.port=" + jmxServiceURL().getPort() + "\n");
      fileWriter.write("broker.0.jmx.port=" + jmxServiceURL().getPort() + "\n");
      fileWriter.write("broker.1.jmx.port=" + jmxServiceURL().getPort() + "\n");
      fileWriter.write("broker.2.jmx.port=" + jmxServiceURL().getPort());
      fileWriter.flush();
      fileWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    props.put(
        "partitioner.config",
        PartitionerTest.class.getResource("").getPath() + "PartitionerConfigTest");
    return props;
  }

  @Test
  void testPartitioner() {
    var topicName = "address";
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
      while (i < 300) {
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
    sleep(1);
    try (var consumer =
        Consumer.builder()
            .brokers(bootstrapServers())
            .fromBeginning()
            .topics(Set.of(topicName))
            .keyDeserializer(Deserializer.STRING)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(20));
      var recordsCount = records.size();
      while (recordsCount < 300) {
        recordsCount += consumer.poll(Duration.ofSeconds(20)).size();
      }
      assertEquals(300, recordsCount);
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
  void testMultipleProducer() {
    var topicName = "addr";
    admin.creator().topic(topicName).numberOfPartitions(10).create();
    var key = "tainan";
    var timestamp = System.currentTimeMillis() + 10;
    var header = Header.of("a", "b".getBytes());
    try (var threadPool =
        ThreadPool.builder()
            .executors(
                IntStream.range(0, 10)
                    .mapToObj(
                        i ->
                            producerExecutor(
                                Producer.builder()
                                    .keySerializer(Serializer.STRING)
                                    .configs(
                                        initProConfig().entrySet().stream()
                                            .collect(
                                                Collectors.toMap(
                                                    e -> e.getKey().toString(),
                                                    Map.Entry::getValue)))
                                    .build(),
                                topicName,
                                key,
                                header,
                                timestamp))
                    .collect(Collectors.toUnmodifiableList()))
            .build()) {
      threadPool.waitAll();
    }
    try (var consumer =
        Consumer.builder()
            .brokers(bootstrapServers())
            .fromBeginning()
            .topics(Set.of(topicName))
            .keyDeserializer(Deserializer.STRING)
            .build()) {
      var records = consumer.poll(Duration.ofSeconds(20));
      var recordsCount = records.size();
      while (recordsCount < 1000) {
        recordsCount += consumer.poll(Duration.ofSeconds(20)).size();
      }
      assertEquals(1000, recordsCount);
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
  void testJmxConfig() {
    var props = initProConfig();
    var file = new File(PartitionerTest.class.getResource("").getPath() + "PartitionerConfigTest");
    try {
      var fileWriter = new FileWriter(file);
      fileWriter.write(
          "broker.0.jmx.port="
              + jmxServiceURL().getPort()
              + "\n"
              + "broker.1.jmx.port="
              + jmxServiceURL().getPort()
              + "\n"
              + "broker.2.jmx.port="
              + jmxServiceURL().getPort()
              + "\n");
      fileWriter.flush();
      fileWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    var topicName = "addressN";
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
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private Executor producerExecutor(
      Producer<String, byte[]> producer, String topic, String key, Header header, long timeStamp) {
    return new Executor() {
      int i = 0;

      @Override
      public State execute() throws InterruptedException {
        if (i > 99) return State.DONE;
        try {
          producer
              .sender()
              .topic(topic)
              .key(key)
              .timestamp(timeStamp)
              .headers(List.of(header))
              .run()
              .toCompletableFuture()
              .get();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
        i++;
        return State.RUNNING;
      }

      @Override
      public void close() {
        producer.close();
      }
    };
  }

  private static void sleep(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetAndChoose() {
    var smoothWeight = new SmoothWeightRoundRobin(Map.of(1, 5.0, 2, 3.0, 3, 1.0));

    Assertions.assertEquals(1, smoothWeight.getAndChoose());
    Assertions.assertEquals(2, smoothWeight.getAndChoose());
    Assertions.assertEquals(3, smoothWeight.getAndChoose());
    Assertions.assertEquals(1, smoothWeight.getAndChoose());
    Assertions.assertEquals(2, smoothWeight.getAndChoose());
    Assertions.assertEquals(3, smoothWeight.getAndChoose());
    Assertions.assertEquals(1, smoothWeight.getAndChoose());
  }
}
