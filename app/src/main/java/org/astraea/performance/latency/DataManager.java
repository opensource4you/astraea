package org.astraea.performance.latency;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.ProducerRecord;

class DataManager {

  static DataManager of(String topic, int valueSize) {
    return new DataManager(topic, valueSize, true);
  }

  static DataManager noConsumer(String topic, int valueSize) {
    return new DataManager(topic, valueSize, false);
  }

  /** record key -> (record, timestamp_done) */
  private final ConcurrentMap<byte[], Map.Entry<ProducerRecord<byte[], byte[]>, Long>>
      sendingRecords = new ConcurrentSkipListMap<>(Arrays::compare);

  private final String topic;
  private final int valueSize;
  private final boolean hasConsumer;

  private final AtomicLong recordIndex = new AtomicLong(0);

  final AtomicLong producerRecords = new AtomicLong(0);

  private DataManager(String topic, int valueSize, boolean hasConsumer) {
    this.topic = Objects.requireNonNull(topic);
    this.valueSize = valueSize;
    this.hasConsumer = hasConsumer;
  }

  /**
   * @return generate a new record with random data and specify topic name. The record consists of
   *     key, value, topic and header.
   */
  ProducerRecord<byte[], byte[]> producerRecord() {
    var content = String.valueOf(recordIndex.getAndIncrement());
    var rawContent = content.getBytes();
    var headers = Collections.singletonList(KafkaUtils.header(content, rawContent));
    return new ProducerRecord<>(
        topic,
        null,
        (long) (Math.random() * System.currentTimeMillis()),
        rawContent,
        new byte[valueSize],
        headers);
  }

  void sendingRecord(ProducerRecord<byte[], byte[]> record, long now) {
    if (hasConsumer) {
      var previous =
          sendingRecords.put(record.key(), new AbstractMap.SimpleImmutableEntry<>(record, now));
      if (previous != null) throw new RuntimeException("duplicate data!!!");
    }
    producerRecords.incrementAndGet();
  }

  /**
   * get sending record
   *
   * @param key of completed record
   * @return the completed record. Or NullPointerException if there is no related record.
   */
  Map.Entry<ProducerRecord<byte[], byte[]>, Long> removeSendingRecord(byte[] key) {
    if (!hasConsumer)
      throw new UnsupportedOperationException(
          "removeSendingRecord is unsupported when there is no consumer");
    return Objects.requireNonNull(sendingRecords.remove(key));
  }

  /** @return number of completed records */
  long numberOfProducerRecords() {
    return producerRecords.get();
  }
}
