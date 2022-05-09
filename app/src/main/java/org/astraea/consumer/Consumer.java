package org.astraea.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/** An interface for polling records. */
public interface Consumer<Key, Value> extends AutoCloseable {

  default Collection<Record<Key, Value>> poll(Duration timeout) {
    return poll(Integer.MAX_VALUE, timeout);
  }

  /**
   * try to poll data until there are enough records to return or the timeout is reached.
   *
   * @param recordCount max number of returned records.
   * @param timeout max time to wait data
   * @return records
   */
  Collection<Record<Key, Value>> poll(int recordCount, Duration timeout);

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

  static Builder<byte[], byte[]> builder() {
    return new Builder<>();
  }

  static Consumer<byte[], byte[]> of(String brokers) {
    return builder().brokers(brokers).build();
  }

  static Consumer<byte[], byte[]> of(Map<String, String> configs) {
    return builder().configs(configs).build();
  }
}
