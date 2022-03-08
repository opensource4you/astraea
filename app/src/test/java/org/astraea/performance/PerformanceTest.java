package org.astraea.performance;

import static org.astraea.performance.Performance.partition;

import com.beust.jcommander.ParameterException;
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

  @Test
  void testArgument() {
    String[] arguments1 = {
      "--bootstrap.servers",
      "localhost:9092",
      "--topic",
      "not-empty",
      "--partitions",
      "10",
      "--replicas",
      "3",
      "--producers",
      "1",
      "--consumers",
      "1",
      "--run.until",
      "1000records",
      "--record.size",
      "10KiB",
      "--partitioner",
      "org.astraea.partitioner.smoothPartitioner.SmoothWeightPartitioner",
      "--compression",
      "lz4",
      "--key.distribution",
      "zipfian",
      "--specify.broker",
      "1",
      "--configs",
      "key=value"
    };

    var arg = org.astraea.argument.Argument.parse(new Performance.Argument(), arguments1);
    Assertions.assertEquals("value", arg.producerProps().get("key").toString());

    String[] arguments2 = {"--bootstrap.servers", "localhost:9092", "--topic", ""};
    Assertions.assertThrows(
        ParameterException.class,
        () -> org.astraea.argument.Argument.parse(new Performance.Argument(), arguments2));

    String[] arguments3 = {"--bootstrap.servers", "localhost:9092", "--replicas", "0"};
    Assertions.assertThrows(
        ParameterException.class,
        () -> org.astraea.argument.Argument.parse(new Performance.Argument(), arguments3));

    String[] arguments4 = {"--bootstrap.servers", "localhost:9092", "--partitions", "0"};
    Assertions.assertThrows(
        ParameterException.class,
        () -> org.astraea.argument.Argument.parse(new Performance.Argument(), arguments4));

    String[] arguments5 = {"--bootstrap.servers", "localhost:9092", "--producers", "0"};
    Assertions.assertThrows(
        ParameterException.class,
        () -> org.astraea.argument.Argument.parse(new Performance.Argument(), arguments5));

    String[] arguments6 = {"--bootstrap.servers", "localhost:9092", "--consumers", "0"};
    Assertions.assertDoesNotThrow(
        () -> org.astraea.argument.Argument.parse(new Performance.Argument(), arguments6));

    String[] arguments7 = {"--bootstrap.servers", "localhost:9092", "--run.until", "1"};
    Assertions.assertThrows(
        ParameterException.class,
        () -> org.astraea.argument.Argument.parse(new Performance.Argument(), arguments7));

    String[] arguments8 = {"--bootstrap.servers", "localhost:9092", "--record.size", "1"};
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> org.astraea.argument.Argument.parse(new Performance.Argument(), arguments8));

    String[] arguments10 = {"--bootstrap.servers", "localhost:9092", "--partitioner", ""};
    Assertions.assertThrows(
        ParameterException.class,
        () -> org.astraea.argument.Argument.parse(new Performance.Argument(), arguments10));

    String[] arguments11 = {"--bootstrap.servers", "localhost:9092", "--compression", ""};
    Assertions.assertThrows(
        ParameterException.class,
        () -> org.astraea.argument.Argument.parse(new Performance.Argument(), arguments11));

    String[] arguments12 = {"--bootstrap.servers", "localhost:9092", "--key.distribution", ""};
    Assertions.assertThrows(
        ParameterException.class,
        () -> org.astraea.argument.Argument.parse(new Performance.Argument(), arguments12));

    String[] arguments13 = {"--bootstrap.servers", "localhost:9092", "--specify.broker", ""};
    Assertions.assertThrows(
        ParameterException.class,
        () -> org.astraea.argument.Argument.parse(new Performance.Argument(), arguments13));
  }
}
