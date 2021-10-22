package org.astraea.performance;

import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** An interface for sending records. */
public interface Producer {
  Future<RecordMetadata> send(byte[] payload);

  void cleanup();

  /**
   * Create a KafkaProducer.
   *
   * @param prop: Properties to create a KafkaProducer
   * @param topic: Topic to send to
   * @return a KafkaProducer
   */
  static Producer fromKafka(Properties prop, String topic) {
    final KafkaProducer<byte[], byte[]> kafkaProducer =
        new KafkaProducer<>(prop, new ByteArraySerializer(), new ByteArraySerializer());
    return new Producer() {

      @Override
      public Future<RecordMetadata> send(byte[] payload) {
        return kafkaProducer.send(new ProducerRecord<>(topic, payload));
      }

      @Override
      public void cleanup() {
        kafkaProducer.close();
      }
    };
  }
}
