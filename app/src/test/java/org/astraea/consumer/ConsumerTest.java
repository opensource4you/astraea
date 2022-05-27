package org.astraea.consumer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.kafka.common.errors.WakeupException;
import org.astraea.Utils;
import org.astraea.producer.Producer;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ConsumerTest extends RequireBrokerCluster {

  private static void produceData(String topic, int size) {
    try (var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      IntStream.range(0, size)
          .forEach(
              i ->
                  producer
                      .sender()
                      .topic(topic)
                      .key(String.valueOf(i).getBytes(StandardCharsets.UTF_8))
                      .run());
      producer.flush();
    }
  }

  @Test
  void testFromBeginning() {
    var recordCount = 100;
    var topic = "testPoll";
    produceData(topic, recordCount);
    try (var consumer =
        Consumer.builder()
            .topics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .fromBeginning()
            .build()) {

      Assertions.assertEquals(
          recordCount, consumer.poll(recordCount, Duration.ofSeconds(10)).size());
    }
  }

  @Test
  void testFromLatest() {
    var topic = "testFromLatest";
    produceData(topic, 1);
    try (var consumer =
        Consumer.builder()
            .topics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .fromLatest()
            .build()) {

      Assertions.assertEquals(0, consumer.poll(Duration.ofSeconds(3)).size());
    }
  }

  @Timeout(7)
  @Test
  void testWakeup() throws InterruptedException {
    var topic = "testWakeup";
    try (var consumer =
        Consumer.builder()
            .topics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .fromLatest()
            .build()) {
      var service = Executors.newSingleThreadExecutor();
      service.execute(
          () -> {
            try {
              TimeUnit.SECONDS.sleep(3);
              consumer.wakeup();
            } catch (InterruptedException ignored) {
              // swallow
            }
          });
      // this call will be broken after 3 seconds
      Assertions.assertThrows(WakeupException.class, () -> consumer.poll(Duration.ofSeconds(100)));

      service.shutdownNow();
      Assertions.assertTrue(service.awaitTermination(3, TimeUnit.SECONDS));
    }
  }

  @Test
  void testGroupId() {
    var groupId = "testGroupId";
    var topic = "testGroupId";
    produceData(topic, 1);

    java.util.function.BiConsumer<String, Integer> testConsumer =
        (id, expectedSize) -> {
          try (var consumer =
              Consumer.builder()
                  .topics(Set.of(topic))
                  .bootstrapServers(bootstrapServers())
                  .fromBeginning()
                  .groupId(id)
                  .build()) {
            Assertions.assertEquals(
                expectedSize, consumer.poll(expectedSize, Duration.ofSeconds(5)).size());
          }
        };

    testConsumer.accept(groupId, 1);

    // the data is fetched already, so it should not return any data
    testConsumer.accept(groupId, 0);

    // use different group id
    testConsumer.accept("another_group", 0);
  }

  @Test
  void testDistanceFromLatest() {
    var count = 10;
    var topic = Utils.randomString(10);
    try (var producer = Producer.of(bootstrapServers())) {
      IntStream.range(0, count)
          .forEach(
              i ->
                  producer
                      .sender()
                      .topic(topic)
                      .value(String.valueOf(count).getBytes(StandardCharsets.UTF_8))
                      .run());
      producer.flush();
    }
    try (var consumer =
        Consumer.builder()
            .bootstrapServers(bootstrapServers())
            .topics(Set.of(topic))
            .distanceFromLatest(3)
            .build()) {
      Assertions.assertEquals(3, consumer.poll(4, Duration.ofSeconds(5)).size());
    }

    try (var consumer =
        Consumer.builder()
            .bootstrapServers(bootstrapServers())
            .topics(Set.of(topic))
            .distanceFromLatest(1000)
            .build()) {
      Assertions.assertEquals(10, consumer.poll(11, Duration.ofSeconds(5)).size());
    }
  }

  @Test
  void testRecordsPollingTime() {
    var count = 1;
    var topic = "testPollingTime";
    try (var consumer =
        Consumer.builder()
            .bootstrapServers(bootstrapServers())
            .topics(Set.of(topic))
            .fromBeginning()
            .build()) {

      // poll() returns immediately, if there is(/are) record(s) to poll.
      produceData(topic, count);
      Assertions.assertTimeout(
          Duration.ofSeconds(10), () -> consumer.poll(Duration.ofSeconds(Integer.MAX_VALUE)));
    }
  }
}
