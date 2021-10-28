package org.astraea.producer;

public interface Metadata {
  /**
   * The size of the serialized, uncompressed key in bytes. If key is null, the returned size is -1.
   */
  long serializedKeySize();

  /**
   * The size of the serialized, uncompressed value in bytes. If value is null, the returned size is
   * -1.
   */
  long serializedValueSize();

  /**
   * The timestamp of the record in the topic/partition.
   *
   * @return the timestamp of the record
   */
  long timestamp();

  /** The topic the record was appended to */
  String topic();

  /** The partition the record was sent to */
  int partition();
}
