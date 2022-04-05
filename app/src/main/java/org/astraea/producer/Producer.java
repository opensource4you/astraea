package org.astraea.producer;

import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.astraea.performance.Metric;

/** An interface for sending records. */
public interface Producer<Key, Value> extends AutoCloseable {
  Sender<Key, Value> sender();
  /** this method is blocked until all data in buffer are sent. */
  void flush();

  void close();

  /** Get a kafkaMetric by name. */
  Metric getMetric(String metricName);

  KafkaProducer<Key, Value> kafkaProducer();

  static Builder<byte[], byte[]> builder() {
    return new Builder<>();
  }

  static Producer<byte[], byte[]> of(String brokers) {
    return builder().brokers(brokers).build();
  }

  static Producer<byte[], byte[]> of(Map<String, Object> configs) {
    return builder().configs(configs).build();
  }
}
