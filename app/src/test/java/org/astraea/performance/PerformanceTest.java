package org.astraea.performance;

import static org.astraea.performance.Performance.partition;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.astraea.Utils;
import org.astraea.concurrent.Executor;
import org.astraea.concurrent.ThreadPool;
import org.astraea.consumer.Consumer;
import org.astraea.producer.Producer;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerformanceTest extends RequireBrokerCluster {

  @Test
  void testSpecifyBrokerProducerExecutor() {
    var admin = TopicAdmin.of(bootstrapServers());
    var topicName = "testConsumerExecutor-" + System.currentTimeMillis();
    admin.creator().topic(topicName).numberOfPartitions(10).create();
    Utils.waitFor(() -> admin.publicTopicNames().contains(topicName));

    var metrics = new Metrics();
    var param = new Performance.Argument();
    param.brokers = bootstrapServers();
    param.topic = topicName;
    param.fixedSize = true;
    param.exeTime = ExeTime.of("100records");
    param.specifyBroker = List.of(0);
    param.consumers = 0;
    param.partitions = 10;
    try (var executor =
        Performance.producerExecutor(
            Producer.builder().brokers(bootstrapServers()).build(),
            param,
            metrics,
            partition(param, admin),
            new Manager(param, List.of(), List.of()))) {
      ThreadPool threadPool = ThreadPool.builder().executor(executor).build();
      threadPool.waitAll();
      threadPool.close();

      Utils.waitFor(() -> metrics.num() == 100);
      var offsets = admin.offsets(Set.of(topicName));
      var partitions =
          offsets.entrySet().stream()
              .filter(entry -> entry.getValue().latest() > 0)
              .map(entry -> entry.getKey().partition())
              .collect(Collectors.toList());
      var partitionsOfBrokers =
          admin.partitionsOfBrokers(Set.of(topicName), Set.of(0)).stream()
              .map(TopicPartition::partition)
              .collect(Collectors.toSet());
      partitions.forEach(
          partition -> Assertions.assertTrue(partitionsOfBrokers.contains(partition)));
    }
  }

  @Test
  void testMultipleSpecifyBrokersProducerExecutor() {
    var admin = TopicAdmin.of(bootstrapServers());
    var topicName = "testConsumerExecutor-" + System.currentTimeMillis();
    admin.creator().topic(topicName).numberOfPartitions(10).create();
    Utils.waitFor(() -> admin.publicTopicNames().contains(topicName));

    var metrics = new Metrics();
    var param = new Performance.Argument();
    param.brokers = bootstrapServers();
    param.topic = topicName;
    param.fixedSize = true;
    param.exeTime = ExeTime.of("100records");
    param.specifyBroker = List.of(0, 1);
    param.consumers = 0;
    param.partitions = 10;
    try (Executor executor =
        Performance.producerExecutor(
            Producer.builder().brokers(bootstrapServers()).build(),
            param,
            metrics,
            partition(param, admin),
            new Manager(param, List.of(), List.of()))) {
      ThreadPool threadPool = ThreadPool.builder().executor(executor).build();
      threadPool.waitAll();
      threadPool.close();

      Utils.waitFor(() -> metrics.num() == 100);
      var offsets = admin.offsets(Set.of(topicName));
      var partitions =
          offsets.entrySet().stream()
              .filter(entry -> entry.getValue().latest() > 0)
              .map(entry -> entry.getKey().partition())
              .collect(Collectors.toList());
      var partitionsOfBrokers =
          admin.partitionsOfBrokers(Set.of(topicName), Set.of(0, 1)).stream()
              .map(TopicPartition::partition)
              .collect(Collectors.toSet());
      partitions.forEach(
          partition -> Assertions.assertTrue(partitionsOfBrokers.contains(partition)));
    }
  }

  @Test
  void testProducerExecutor() throws InterruptedException {
    var metrics = new Metrics();
    var param = new Performance.Argument();
    param.brokers = bootstrapServers();
    param.topic = "testProducerExecutor-" + System.currentTimeMillis();
    param.fixedSize = true;
    param.consumers = 0;
    try (Executor executor =
        Performance.producerExecutor(
            Producer.builder().brokers(bootstrapServers()).build(),
            param,
            metrics,
            List.of(-1),
            new Manager(param, List.of(), List.of()))) {
      executor.execute();

      Utils.waitFor(() -> metrics.num() == 1);
      Assertions.assertEquals(1024, metrics.bytes());
    }
  }

  @Test
  void testConsumerExecutor() throws InterruptedException, ExecutionException {
    Metrics metrics = new Metrics();
    var topicName = "testConsumerExecutor-" + System.currentTimeMillis();
    var param = new Performance.Argument();
    param.fixedSize = true;
    try (Executor executor =
        Performance.consumerExecutor(
            Consumer.builder().topics(Set.of(topicName)).brokers(bootstrapServers()).build(),
            metrics,
            new Manager(param, List.of(), List.of()))) {
      executor.execute();

      Assertions.assertEquals(0, metrics.num());
      Assertions.assertEquals(0, metrics.bytes());

      try (var producer = Producer.builder().brokers(bootstrapServers()).build()) {
        producer.sender().topic(topicName).value(new byte[1024]).run().toCompletableFuture().get();
      }
      executor.execute();

      Assertions.assertEquals(1, metrics.num());
      Assertions.assertNotEquals(1024, metrics.bytes());
    }
  }
}
