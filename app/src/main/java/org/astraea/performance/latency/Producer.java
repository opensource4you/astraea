package org.astraea.performance.latency;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

interface Producer extends Closeable {

  static Producer fromKafka(Map<String, Object> props) {
    var kafkaProducer =
        new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
    return new Producer() {

      @Override
      public CompletionStage<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
        var f = new CompletableFuture<RecordMetadata>();
        kafkaProducer.send(
            record,
            (r, e) -> {
              if (e != null) f.completeExceptionally(e);
              else f.complete(r);
            });
        return f;
      }

      @Override
      public void flush() {
        kafkaProducer.flush();
      }

      @Override
      public void close() {
        kafkaProducer.close();
      }
    };
  }

  /** see {@link KafkaProducer#send(ProducerRecord)} */
  CompletionStage<RecordMetadata> send(ProducerRecord<byte[], byte[]> record);

  /** see {@link KafkaProducer#flush()}} */
  default void flush() {}

  @Override
  default void close() {}
}
