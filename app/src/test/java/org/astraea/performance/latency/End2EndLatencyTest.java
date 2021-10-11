package org.astraea.performance.latency;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class End2EndLatencyTest {

  private final String topic = "topic-" + System.currentTimeMillis();
  private final DataManager dataManager = DataManager.of(Set.of(topic), 10);

  @Test
  void testExecute() throws Exception {
    var factory = new FakeComponentFactory();
    var parameters = new End2EndLatency.Argument();
    parameters.brokers = "brokers";
    parameters.topics = Set.of("topic");
    parameters.numberOfProducers = 1;
    parameters.numberOfConsumers = 1;
    parameters.duration = Duration.ofSeconds(1);
    parameters.valueSize = 10;
    parameters.flushDuration = Duration.ofSeconds(1);
    try (var r = End2EndLatency.execute(factory, parameters)) {
      TimeUnit.SECONDS.sleep(2);
    }

    // check producers count
    Assertions.assertTrue(factory.producerSendCount.get() > 0);
    Assertions.assertTrue(factory.producerFlushCount.get() > 0);
    Assertions.assertEquals(1, factory.producerCloseCount.get());

    // check consumer count
    Assertions.assertTrue(factory.consumerPoolCount.get() > 0);
    Assertions.assertEquals(1, factory.consumerWakeupCount.get());
    Assertions.assertEquals(1, factory.consumerCloseCount.get());

    // check admin topic count
    Assertions.assertEquals(1, factory.topicAdminListCount.get());
    Assertions.assertEquals(1, factory.topicAdminCloseCount.get());
    Assertions.assertEquals(1, factory.topicAdminCreateCount.get());
  }

  @Test
  void testProducerExecutor() throws InterruptedException {
    var count = new AtomicInteger(0);
    try (Producer producer =
        record -> {
          count.incrementAndGet();
          return CompletableFuture.completedFuture(
              new RecordMetadata(new TopicPartition(record.topic(), 1), 1L, 1L, 1L, 1L, 1, 1));
        }) {
      var thread =
          End2EndLatency.producerThread(
              dataManager, new MeterTracker("test producer"), producer, Duration.ZERO);
      thread.execute();
      Assertions.assertEquals(1, count.get());
      Assertions.assertEquals(1, dataManager.numberOfProducerRecords());
    }
  }

  @Test
  void testConsumerExecutor() throws InterruptedException {
    var count = new AtomicInteger(0);
    var producerRecords = dataManager.producerRecords();
    var consumerRecords =
        producerRecords.stream()
            .map(FakeComponentFactory::toConsumerRecord)
            .collect(Collectors.toList());
    try (Consumer consumer =
        () -> {
          count.incrementAndGet();
          return new ConsumerRecords<>(
              Collections.singletonMap(new TopicPartition(topic, 1), consumerRecords));
        }) {
      var tracker = new MeterTracker("test consumer");
      Assertions.assertEquals(0, tracker.maxLatency());
      Assertions.assertEquals(0, tracker.averageLatency());

      var thread = End2EndLatency.consumerExecutor(dataManager, tracker, consumer);
      dataManager.sendingRecord(producerRecords, System.currentTimeMillis());

      // add the latency
      TimeUnit.SECONDS.sleep(1);
      thread.execute();
      Assertions.assertEquals(1, count.get());
      Assertions.assertEquals(1, dataManager.numberOfProducerRecords());
      Assertions.assertEquals(1, tracker.count());
      Assertions.assertNotEquals(0, tracker.maxLatency());
      Assertions.assertNotEquals(0, tracker.averageLatency());
    }
  }
}
