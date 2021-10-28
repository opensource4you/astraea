package org.astraea.consumer;

public interface Record<Key, Value> {
  /** The topic this record is received from (never null) */
  String topic();

  /** The partition from which this record is received */
  int partition();

  /** The key (or null if no key is specified) */
  Key key();

  /** The value */
  Value value();

  /** The position of this record in the corresponding Kafka partition. */
  long offset();

  /** The timestamp of this record */
  long timestamp();

  /**
   * The size of the serialized, uncompressed key in bytes. If key is null, the returned size is -1.
   */
  int serializedKeySize();

  /**
   * The size of the serialized, uncompressed value in bytes. If value is null, the returned size is
   * -1.
   */
  int serializedValueSize();
}
