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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.astraea.common.Header;
import org.astraea.common.admin.TopicPartition;

/**
 * @param offset The position of this record in the corresponding Kafka partition.
 * @param timestamp timestamp of record
 * @param partition expected partition, or null if you don't care for it.
 * @param serializedKeySize The size of the serialized, uncompressed key in bytes. If key is null,
 *     the returned size is -1.
 * @param serializedValueSize The size of the serialized, uncompressed value in bytes. If value is
 *     null, the returned size is -1.
 * @param leaderEpoch the leader epoch or empty for legacy record formats
 */
public record Record<Key, Value>(
    String topic,
    List<Header> headers,
    Key key,
    Value value,
    long offset,
    long timestamp,
    int partition,
    int serializedKeySize,
    int serializedValueSize,
    Optional<Integer> leaderEpoch) {

  public static <Key, Value> RecordBuilder<Key, Value> builder() {
    return new RecordBuilder<>();
  }

  public TopicPartition topicPartition() {
    return TopicPartition.of(topic(), partition());
  }

  public static class RecordBuilder<Key, Value> {

    private Object key;
    private Object value;
    private long offset;
    private String topic;
    private int partition;
    private long timestamp;
    private List<Header> headers = List.of();

    private int serializedKeySize;
    private int serializedValueSize;

    private Optional<Integer> leaderEpoch = Optional.empty();

    private RecordBuilder() {}

    @SuppressWarnings("unchecked")
    public <NewKey> RecordBuilder<NewKey, Value> key(NewKey key) {
      this.key = key;
      return (RecordBuilder<NewKey, Value>) this;
    }

    @SuppressWarnings("unchecked")
    public <NewValue> RecordBuilder<Key, NewValue> value(NewValue value) {
      this.value = value;
      return (RecordBuilder<Key, NewValue>) this;
    }

    public RecordBuilder<Key, Value> topicPartition(TopicPartition topicPartition) {
      topic(topicPartition.topic());
      return partition(topicPartition.partition());
    }

    public RecordBuilder<Key, Value> topic(String topic) {
      this.topic = Objects.requireNonNull(topic);
      return this;
    }

    public RecordBuilder<Key, Value> partition(int partition) {
      if (partition >= 0) this.partition = partition;
      return this;
    }

    public RecordBuilder<Key, Value> timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public RecordBuilder<Key, Value> headers(List<Header> headers) {
      this.headers = headers;
      return this;
    }

    public RecordBuilder<Key, Value> serializedKeySize(int serializedKeySize) {
      this.serializedKeySize = serializedKeySize;
      return this;
    }

    public RecordBuilder<Key, Value> serializedValueSize(int serializedValueSize) {
      this.serializedValueSize = serializedValueSize;
      return this;
    }

    public RecordBuilder<Key, Value> leaderEpoch(Optional<Integer> leaderEpoch) {
      this.leaderEpoch = leaderEpoch;
      return this;
    }

    public RecordBuilder<Key, Value> offset(long offset) {
      this.offset = offset;
      return this;
    }

    @SuppressWarnings("unchecked")
    public Record<Key, Value> build() {
      return new Record<>(
          Objects.requireNonNull(RecordBuilder.this.topic),
          Objects.requireNonNull(RecordBuilder.this.headers),
          (Key) RecordBuilder.this.key,
          (Value) RecordBuilder.this.value,
          RecordBuilder.this.offset,
          RecordBuilder.this.timestamp,
          RecordBuilder.this.partition,
          RecordBuilder.this.serializedKeySize,
          RecordBuilder.this.serializedValueSize,
          Objects.requireNonNull(RecordBuilder.this.leaderEpoch));
    }
  }
}
