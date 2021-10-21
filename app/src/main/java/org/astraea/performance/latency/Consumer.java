package org.astraea.performance.latency;

import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

interface Consumer extends Closeable {
  Duration POLL_TIMEOUT = Duration.ofMillis(500);

  static Consumer fromKafka(Map<String, Object> props, Set<String> topics) {
    var kafkaConsumer =
        new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    kafkaConsumer.subscribe(topics);
    return new Consumer() {

      @Override
      public ConsumerRecords<byte[], byte[]> poll() {
        return kafkaConsumer.poll(POLL_TIMEOUT);
      }

      @Override
      public void wakeup() {
        kafkaConsumer.wakeup();
      }

      @Override
      public void close() {
        kafkaConsumer.close();
      }
    };
  }

  /** see {@link KafkaConsumer#poll(Duration)} */
  ConsumerRecords<byte[], byte[]> poll();

  /** see {@link KafkaConsumer#wakeup()} */
  default void wakeup() {}

  @Override
  default void close() {}
}
