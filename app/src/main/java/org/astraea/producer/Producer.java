package org.astraea.producer;

/** An interface for sending records. */
public interface Producer<Key, Value> extends AutoCloseable {
  Sender<Key, Value> sender();

  void close();

  static Builder<byte[], byte[]> builder() {
    return new Builder<>();
  }
}
