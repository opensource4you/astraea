package org.astraea.dependency;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

/**
 * Each group of order records uses the same DependencyRecord instance.
 *
 * <pre>{@code
 * DependencyRecord dependencyRecord = new DependencyRecord();
 * KafkaProducer<String, String> producer = new KafkaProducer<>(props);
 * producer.send(dependencyRecord.orderRecord());
 * producer.send(dependencyRecord.orderRecord());
 * producer.send(dependencyRecord.orderRecord());
 * }</pre>
 */
public class DependencyRecord<K, V> {
  private final int partition;

  public DependencyRecord() {
    // TODO
    this.partition = 0;
  }

  /**
   * Creates a record with a specified timestamp to be sent to a specified topic and partition
   *
   * @param topic The topic the record will be appended to
   * @param partition The partition to which the record should be sent
   * @param timestamp The timestamp of the record, in milliseconds since epoch. If null, the
   *     producer will assign the timestamp using System.currentTimeMillis().
   * @param key The key that will be included in the record
   * @param value The record contents
   */
  public ProducerRecord<K, V> orderRecord(
      String topic, Integer partition, Long timestamp, K key, V value) {
    return new ProducerRecord<>(topic, partition, timestamp, key, value, null);
  }

  /**
   * Creates a record to be sent to a specified topic and partition
   *
   * @param topic The topic the record will be appended to
   * @param partition The partition to which the record should be sent
   * @param key The key that will be included in the record
   * @param value The record contents
   * @param headers The headers that will be included in the record
   */
  public ProducerRecord<K, V> orderRecord(
      String topic, Integer partition, K key, V value, Iterable<Header> headers) {
    return new ProducerRecord<>(topic, partition, null, key, value, headers);
  }

  /**
   * Creates a record to be sent to a specified topic and partition
   *
   * @param topic The topic the record will be appended to
   * @param partition The partition to which the record should be sent
   * @param key The key that will be included in the record
   * @param value The record contents
   */
  public ProducerRecord<K, V> orderRecord(String topic, Integer partition, K key, V value) {
    return new ProducerRecord<>(topic, partition, null, key, value, null);
  }

  /**
   * Create a record to be sent to Kafka
   *
   * @param topic The topic the record will be appended to
   * @param key The key that will be included in the record
   * @param value The record contents
   */
  public ProducerRecord<K, V> orderRecord(String topic, K key, V value) {
    return new ProducerRecord<>(topic, partition, null, key, value, null);
  }

  /**
   * Create a record with no key
   *
   * @param topic The topic this record should be sent to
   * @param value The record contents
   */
  public ProducerRecord<K, V> orderRecord(String topic, V value) {
    return new ProducerRecord<>(topic, partition, null, null, value, null);
  }
}
