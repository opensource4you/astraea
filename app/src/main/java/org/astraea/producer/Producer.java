package org.astraea.producer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/** An interface for sending records. */
public interface Producer<Key, Value> extends AutoCloseable {
  Sender<Key, Value> sender();

  Collection<CompletionStage<Metadata>> transaction(Collection<Sender<Key, Value>> senders);

  /** this method is blocked until all data in buffer are sent. */
  void flush();

  void close();

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
