package org.astraea.performance;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** An interface for sending records. */
public interface Producer extends AutoCloseable {
  CompletionStage<RecordMetadata> send(byte[] payload);

  void close();

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
      public CompletionStage<RecordMetadata> send(byte[] payload) {
        var completableFuture = new CompletableFuture<RecordMetadata>();
        kafkaProducer.send(
            new ProducerRecord<>(topic, payload),
            (metadata, exception) -> {
              if (exception == null) completableFuture.complete(metadata);
              else completableFuture.completeExceptionally(exception);
            });
        return completableFuture;
      }

      @Override
      public void close() {
        kafkaProducer.close();
      }
    };
  }
}
