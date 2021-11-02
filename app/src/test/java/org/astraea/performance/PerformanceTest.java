package org.astraea.performance;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.astraea.Utils;
import org.astraea.concurrent.ThreadPool;
import org.astraea.consumer.Consumer;
import org.astraea.producer.Producer;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerformanceTest extends RequireBrokerCluster {

  @Test
  void testProducerExecutor() throws InterruptedException {
    var metrics = new Metrics();
    var param = new Performance.Argument();
    param.topic = "testProducerExecutor-" + System.currentTimeMillis();
    try (ThreadPool.Executor executor =
        Performance.producerExecutor(
            Producer.builder().brokers(bootstrapServers()).build(),
            param,
            metrics,
            new AtomicLong(10),
            new CountDownLatch(0))) {
      executor.execute();

      Utils.waitFor(() -> metrics.num() == 1);
      Assertions.assertEquals(1024, metrics.bytes());
    }
  }

  @Test
  void testConsumerExecutor() throws InterruptedException, ExecutionException {
    Metrics metrics = new Metrics();
    var topicName = "testConsumerExecutor-" + System.currentTimeMillis();
    try (ThreadPool.Executor executor =
        Performance.consumerExecutor(
            Consumer.builder().topics(Set.of(topicName)).brokers(bootstrapServers()).build(),
            metrics,
            new AtomicLong(10))) {
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
