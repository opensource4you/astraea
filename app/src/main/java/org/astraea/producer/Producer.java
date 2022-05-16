package org.astraea.producer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/** An interface for sending records. */
public interface Producer<Key, Value> extends AutoCloseable {
  Sender<Key, Value> sender();

  /**
   * send the multiple records. Noted that the normal producer will send the record one by one. By
   * contrast, transactional producer will send all records in single transaction.
   *
   * @param senders pre-defined records
   * @return callback of all completed records
   */
  Collection<CompletionStage<Metadata>> send(Collection<Sender<Key, Value>> senders);

  /** this method is blocked until all data in buffer are sent. */
  void flush();

  void close();

  /** @return true if the producer supports transactional. */
  boolean transactional();

  static Builder<byte[], byte[]> builder() {
    return new Builder<>();
  }

  static Producer<byte[], byte[]> of(String bootstrapServers) {
    return builder().bootstrapServers(bootstrapServers).build();
  }

  static Producer<byte[], byte[]> of(Map<String, String> configs) {
    return builder().configs(configs).build();
  }
}
