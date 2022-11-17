/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.consumer;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.astraea.common.Header;

public final class Record<Key, Value> {

  static <Key, Value> Record<Key, Value> of(ConsumerRecord<Key, Value> record) {
    return new Record<>(
        record.topic(),
        record.partition(),
        record.offset(),
        record.timestamp(),
        record.serializedKeySize(),
        record.serializedValueSize(),
        Header.of(record.headers()),
        record.key(),
        record.value(),
        record.leaderEpoch());
  }

  private final String topic;
  private final int partition;
  private final long offset;
  private final long timestamp;
  private final int serializedKeySize;
  private final int serializedValueSize;
  private final List<Header> headers;
  private final Key key;
  private final Value value;
  private final Optional<Integer> leaderEpoch;

  public Record(
      String topic,
      int partition,
      long offset,
      long timestamp,
      int serializedKeySize,
      int serializedValueSize,
      List<Header> headers,
      Key key,
      Value value,
      Optional<Integer> leaderEpoch) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.serializedKeySize = serializedKeySize;
    this.serializedValueSize = serializedValueSize;
    this.headers = Collections.unmodifiableList(headers);
    this.key = key;
    this.value = value;
    this.leaderEpoch = leaderEpoch;
  }

  /** The topic this record is received from (never null) */
  public String topic() {
    return topic;
  }

  /** The partition from which this record is received */
  public int partition() {
    return partition;
  }

  /** The key (or null if no key is specified) */
  public Key key() {
    return key;
  }

  /** The value */
  public Value value() {
    return value;
  }

  /** The position of this record in the corresponding Kafka partition. */
  public long offset() {
    return offset;
  }

  /** The timestamp of this record */
  public long timestamp() {
    return timestamp;
  }

  /**
   * The size of the serialized, uncompressed key in bytes. If key is null, the returned size is -1.
   */
  public int serializedKeySize() {
    return serializedKeySize;
  }

  /**
   * The size of the serialized, uncompressed value in bytes. If value is null, the returned size is
   * -1.
   */
  public int serializedValueSize() {
    return serializedValueSize;
  }

  /** The headers (never null) */
  public List<Header> headers() {
    return headers;
  }

  /**
   * Get the leader epoch for the record if available
   *
   * @return the leader epoch or empty for legacy record formats
   */
  public Optional<Integer> leaderEpoch() {
    return leaderEpoch;
  }
}
