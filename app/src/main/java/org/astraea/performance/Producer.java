package org.astraea.performance;

import java.util.Properties;
import java.util.concurrent.Future;
import java.io.Closeable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public interface Producer {
  Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> producerRecord);
  void close();
  static Producer fromKafka(Properties prop) {
    final KafkaProducer<byte[], byte[]> kafkaProducer =
        new KafkaProducer<byte[], byte[]>(
            prop, new ByteArraySerializer(), new ByteArraySerializer());
    return new Producer() {

      @Override
      public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> producerRecord) {
        return kafkaProducer.send(producerRecord);
      }

      @Override
      public void close() {
        kafkaProducer.close();
      }
    };
  }
}
