package org.astraea.partitioner.smoothPartitioner;

import static org.astraea.Utils.requireField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
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
import org.astraea.Utils;
import org.astraea.concurrent.Executor;
import org.astraea.concurrent.State;
import org.astraea.concurrent.ThreadPool;
import org.astraea.consumer.Consumer;
import org.astraea.consumer.Deserializer;
import org.astraea.consumer.Header;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;
import org.astraea.producer.Producer;
import org.astraea.producer.Serializer;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PartitionerTest extends RequireBrokerCluster {
  private final String brokerList = bootstrapServers();
  TopicAdmin admin = TopicAdmin.of(bootstrapServers());

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
  void testSetBrokerHashMap() {
    var nodeLoadClient = Mockito.mock(NodeLoadClient.class);
    when(nodeLoadClient.thoughPutComparison(anyInt())).thenReturn(1.0);
    var poissonMap = new HashMap<Integer, Double>();
    poissonMap.put(0, 0.5);
    poissonMap.put(1, 0.8);
    poissonMap.put(2, 0.3);

    var smoothWeightPartitioner = new SmoothWeightPartitioner();
    setNodeLoadClient(nodeLoadClient, smoothWeightPartitioner);
    smoothWeightPartitioner.brokersWeight(poissonMap);

    var brokerWeight =
        (Map<Integer, SmoothWeightPartitioner.SmoothWeightServer>)
            requireField(smoothWeightPartitioner, "brokersWeight");
    assertEquals(brokerWeight.get(0).originalWeight(), 10);
    assertEquals((brokerWeight.get(1).originalWeight()), 1);
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
  void testUpdateWeightIfNeed() throws NoSuchFieldException, IllegalAccessException {
    SmoothWeightPartitioner smoothWeightPartitioner = new SmoothWeightPartitioner();
    var loadCount = new HashMap<Integer, Integer>();
    loadCount.put(0, 10);
    loadCount.put(1, 12);
    var nodeLoadClient = mock(NodeLoadClient.class);
    var field = smoothWeightPartitioner.getClass().getDeclaredField("nodeLoadClient");
    field.setAccessible(true);
    field.set(smoothWeightPartitioner, nodeLoadClient);
    when(nodeLoadClient.thoughPutComparison(anyInt())).thenReturn(1.0);
    smoothWeightPartitioner.updateWeightIfNeed(loadCount);
    var firstBrokersWeight = smoothWeightPartitioner.brokersWeight().get(0).currentWeight();
    smoothWeightPartitioner.updateWeightIfNeed(loadCount);
    var secondBrokersWeight = smoothWeightPartitioner.brokersWeight().get(0).currentWeight();
    Assertions.assertEquals(firstBrokersWeight, secondBrokersWeight);
    loadCount.put(0, 5);
    loadCount.put(2, 5);
    var lastTime = System.currentTimeMillis();
    Utils.waitFor(() -> Utils.overSecond(lastTime, 1));
    smoothWeightPartitioner.updateWeightIfNeed(loadCount);
    var thirdBrokersWeight = smoothWeightPartitioner.brokersWeight().get(0).currentWeight();
    Assertions.assertNotEquals(secondBrokersWeight, thirdBrokersWeight);
  }

  @Test
  void testUpdateOriginalWeight() {
    var smoothWeightServer = new SmoothWeightPartitioner.SmoothWeightServer(1, 1);

    Assertions.assertEquals(smoothWeightServer.updateOriginalWeight(5), 1);
    Assertions.assertEquals(smoothWeightServer.updateOriginalWeight(5), -4);
    Assertions.assertEquals(smoothWeightServer.originalWeight(), -9);
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

  private void setNodeLoadClient(
      NodeLoadClient nodeLoadClient, SmoothWeightPartitioner partitioner) {
    var target = field(partitioner, "nodeLoadClient");
    try {
      target.set(partitioner, nodeLoadClient);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  private Field field(Object object, String fieldName) {
    Field field = null;
    try {
      field = object.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    }
    return field;
  }

  private static void sleep(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
