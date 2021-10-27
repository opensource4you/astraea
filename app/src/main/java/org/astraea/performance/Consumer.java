package org.astraea.performance;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** An interface for polling records. */
public interface Consumer extends AutoCloseable {

  ConsumerRecords<byte[], byte[]> poll(Duration timeout);

  /**
   * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long
   * poll. The thread which is blocking in an operation will throw {@link
   * org.apache.kafka.common.errors.WakeupException}. If no thread is blocking in a method which can
   * throw {@link org.apache.kafka.common.errors.WakeupException}, the next call to such a method
   * will raise it instead.
   */
  void wakeup();

  @Override
  void close();

  /** Create a Consumer with KafkaConsumer<byte[], byte[]> functionality */
  static Consumer fromKafka(Properties prop, Collection<String> topics) {

    var kafkaConsumer =
        new KafkaConsumer<>(prop, new ByteArrayDeserializer(), new ByteArrayDeserializer());

    kafkaConsumer.subscribe(topics);
    return new Consumer() {

      @Override
      public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
        return kafkaConsumer.poll(timeout);
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
}
