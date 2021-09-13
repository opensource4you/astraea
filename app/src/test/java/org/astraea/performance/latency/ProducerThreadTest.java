package org.astraea.performance.latency;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ProducerThreadTest {
  private final String topic = "topic-" + String.valueOf(System.currentTimeMillis());
  private final DataManager dataManager = DataManager.of(topic, 10);

  @Test
  void testExecute() {
    var count = new AtomicInteger(0);
    try (Producer producer =
        record -> {
          count.incrementAndGet();
          return CompletableFuture.completedFuture(
              new RecordMetadata(new TopicPartition(record.topic(), 1), 1L, 1L, 1L, 1L, 1, 1));
        }) {
      var thread =
          new ProducerThread(
              dataManager, new MeterTracker("test producer"), producer, Duration.ZERO);
      thread.execute();
      Assertions.assertEquals(1, count.get());
      Assertions.assertEquals(1, dataManager.numberOfProducerRecords());
    }
  }
}
