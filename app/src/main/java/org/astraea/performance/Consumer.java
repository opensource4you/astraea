package org.astraea.performance;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public interface Consumer {
  ConsumerRecords<byte[], byte[]> poll(Duration timeout);

  void close();

  /** Create a Consumer with KafkaConsumer<byte[], byte[]> functionality */
  static Consumer fromKafka(Properties prop, Collection<String> topics) {
    KafkaConsumer<byte[], byte[]> kafkaConsumer =
        new KafkaConsumer<byte[], byte[]>(
            prop, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    kafkaConsumer.subscribe(topics);
    return new Consumer() {

      @Override
      public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
        return kafkaConsumer.poll(timeout);
      }

      @Override
      public void close() {
        kafkaConsumer.close();
      }
    };
  }
}
