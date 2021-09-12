package org.astraea.performance.latency;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConsumerThreadTest {
  private final String topic = "topic-" + System.currentTimeMillis();
  private final DataManager dataManager = DataManager.of(topic, 10);

  @Test
  void testExecute() throws InterruptedException {
    var count = new AtomicInteger(0);
    var producerRecord = dataManager.producerRecord();
    var consumerRecord = FakeComponentFactory.toConsumerRecord(producerRecord);
    try (Consumer consumer =
        () -> {
          count.incrementAndGet();
          return new ConsumerRecords<>(
              Collections.singletonMap(
                  new TopicPartition(topic, 1), Collections.singletonList(consumerRecord)));
        }) {
      var tracker = new MeterTracker("test consumer");
      Assertions.assertEquals(0, tracker.maxLatency());
      Assertions.assertEquals(0, tracker.averageLatency());

      var thread = new ConsumerThread(dataManager, tracker, consumer);
      dataManager.sendingRecord(producerRecord, System.currentTimeMillis());

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
