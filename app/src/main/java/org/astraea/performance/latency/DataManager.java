package org.astraea.performance.latency;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

class DataManager {

  private static final int NUMBER_OF_CACHED_VALUES = 100;
  private static final int NUMBER_OF_CACHED_HEADERS = 100;
  private static final int NUMBER_OF_SENDING_HEADERS = 3;

  static DataManager of(Set<String> topics, int valueSize) {
    return new DataManager(topics, valueSize, true);
  }

  static DataManager noConsumer(Set<String> topics, int valueSize) {
    return new DataManager(topics, valueSize, false);
  }

  /** record key -> (record, timestamp_done) */
  private final ConcurrentMap<byte[], Map.Entry<ProducerRecord<byte[], byte[]>, Long>>
      sendingRecords = new ConcurrentSkipListMap<>(Arrays::compare);

  private final Set<String> topics;
  private final boolean hasConsumer;
  private final List<byte[]> values;
  private final List<Header> headers;

  private final AtomicLong recordIndex = new AtomicLong(0);

  final AtomicLong producerRecords = new AtomicLong(0);

  private DataManager(Set<String> topics, int valueSize, boolean hasConsumer) {
    this.topics = Objects.requireNonNull(topics);
    this.values =
        IntStream.range(0, NUMBER_OF_CACHED_VALUES)
            .mapToObj(i -> randomString(valueSize).getBytes())
            .collect(Collectors.toList());
    this.headers =
        IntStream.range(0, NUMBER_OF_CACHED_HEADERS)
            .mapToObj(i -> KafkaUtils.header(randomString(5), randomString(5).getBytes()))
            .collect(Collectors.toList());
    this.hasConsumer = hasConsumer;
  }

  static String randomString(int length) {
    return new Random()
        .ints('a', 'z')
        .limit(length)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  /**
   * @return generate a new record with random data and specify topic name. The record consists of
   *     key, value, topic and headers.
   */
  List<ProducerRecord<byte[], byte[]>> producerRecords() {
    return topics.stream()
        .map(
            topic ->
                new ProducerRecord<>(
                    topic,
                    null,
                    (long) (Math.random() * System.currentTimeMillis()),
                    String.format("%020d", recordIndex.getAndIncrement()).getBytes(),
                    values.get((int) (Math.random() * values.size())),
                    IntStream.range(0, NUMBER_OF_SENDING_HEADERS)
                        .mapToObj(i -> headers.get((int) (Math.random() * headers.size())))
                        .collect(Collectors.toList())))
        .collect(Collectors.toList());
  }

  void sendingRecord(List<ProducerRecord<byte[], byte[]>> records, long now) {
    if (hasConsumer) {
      records.forEach(
          record -> {
            var previous =
                sendingRecords.put(
                    record.key(), new AbstractMap.SimpleImmutableEntry<>(record, now));
            if (previous != null) throw new RuntimeException("duplicate data!!!");
          });
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
