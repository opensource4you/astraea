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
package org.astraea.connector.backup;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.astraea.common.Header;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.consumer.Record;

public class RecordBuilder<Key, Value> {

  public static <Key, Value> RecordBuilder<Key, Value> of() {
    return new RecordBuilder<>();
  }

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
    return new Record<>() {
      private final Key key = (Key) RecordBuilder.this.key;
      private final Value value = (Value) RecordBuilder.this.value;
      private final String topic = Objects.requireNonNull(RecordBuilder.this.topic);

      private final long offset = RecordBuilder.this.offset;
      private final int partition = RecordBuilder.this.partition;
      private final long timestamp = RecordBuilder.this.timestamp;
      private final List<Header> headers = Objects.requireNonNull(RecordBuilder.this.headers);

      private final int serializedKeySize = RecordBuilder.this.serializedKeySize;
      private final int serializedValueSize = RecordBuilder.this.serializedValueSize;

      private final Optional<Integer> leaderEpoch =
          Objects.requireNonNull(RecordBuilder.this.leaderEpoch);

      @Override
      public String topic() {
        return topic;
      }

      @Override
      public List<Header> headers() {
        return headers;
      }

      @Override
      public Key key() {
        return key;
      }

      @Override
      public Value value() {
        return value;
      }

      @Override
      public long offset() {
        return offset;
      }

      @Override
      public long timestamp() {
        return timestamp;
      }

      @Override
      public int partition() {
        return partition;
      }

      @Override
      public int serializedKeySize() {
        return serializedKeySize;
      }

      @Override
      public int serializedValueSize() {
        return serializedValueSize;
      }

      @Override
      public Optional<Integer> leaderEpoch() {
        return leaderEpoch;
      }
    };
  }
}
