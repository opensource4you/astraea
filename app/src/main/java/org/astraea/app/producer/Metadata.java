package org.astraea.app.producer;

import org.apache.kafka.clients.producer.RecordMetadata;

public final class Metadata {

  static Metadata of(RecordMetadata metadata) {
    return new Metadata(
        metadata.offset(),
        metadata.timestamp(),
        metadata.serializedKeySize(),
        metadata.serializedValueSize(),
        metadata.topic(),
        metadata.partition());
  }

  private final long offset;
  private final long timestamp;
  private final int serializedKeySize;
  private final int serializedValueSize;
  private final String topic;
  private final int partition;

  Metadata(
      long offset,
      long timestamp,
      int serializedKeySize,
      int serializedValueSize,
      String topic,
      int partition) {
    this.offset = offset;
    this.timestamp = timestamp;
    this.serializedKeySize = serializedKeySize;
    this.serializedValueSize = serializedValueSize;
    this.topic = topic;
    this.partition = partition;
  }

  /**
   * The offset of the record in the topic/partition.
   *
   * @return the offset of the record, or -1
   */
  public long offset() {
    return this.offset;
  }

  /**
   * @return The size of the serialized, uncompressed key in bytes. If key is null, the returned
   *     size is -1.
   */
  public long serializedKeySize() {
    return serializedKeySize;
  }

  /**
   * @return The size of the serialized, uncompressed value in bytes. If value is null, the returned
   *     size is -1.
   */
  public long serializedValueSize() {
    return serializedValueSize;
  }

  /** @return the timestamp of the record */
  public long timestamp() {
    return timestamp;
  }

  /** @return The topic the record was appended to */
  public String topic() {
    return topic;
  }

  /** @return The partition the record was sent to */
  public int partition() {
    return partition;
  }
}
