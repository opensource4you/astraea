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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.astraea.common.Header;
import org.astraea.common.admin.TopicPartition;

public interface Record<Key, Value> {

  static <Key, Value> Record<Key, Value> of(ConsumerRecord<Key, Value> record) {
    return builder()
        .topic(record.topic())
        .partition(record.partition())
        .timestamp(record.timestamp())
        .offset(record.offset())
        .serializedKeySize(record.serializedKeySize())
        .serializedValueSize(record.serializedValueSize())
        .headers(Header.of(record.headers()))
        .key(record.key())
        .value(record.value())
        .leaderEpoch(record.leaderEpoch())
        .build();
  }

  static Builder<byte[], byte[]> builder() {
    return new Builder<>();
  }

  default TopicPartition topicPartition() {
    return TopicPartition.of(topic(), partition());
  }

  String topic();

  List<Header> headers();

  Key key();

  Value value();

  /** The position of this record in the corresponding Kafka partition. */
  long offset();
  /**
   * @return timestamp of record
   */
  long timestamp();

  /**
   * @return expected partition, or null if you don't care for it.
   */
  int partition();

  /**
   * The size of the serialized, uncompressed key in bytes. If key is null, the returned size is -1.
   */
  int serializedKeySize();

  /**
   * The size of the serialized, uncompressed value in bytes. If value is null, the returned size is
   * -1.
   */
  int serializedValueSize();

  /**
   * Get the leader epoch for the record if available
   *
   * @return the leader epoch or empty for legacy record formats
   */
  Optional<Integer> leaderEpoch();

  class Builder<Key, Value> {
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

    private Builder() {}

    @SuppressWarnings("unchecked")
    public <NewKey> Builder<NewKey, Value> key(NewKey key) {
      this.key = key;
      return (Builder<NewKey, Value>) this;
    }

    @SuppressWarnings("unchecked")
    public <NewValue> Builder<Key, NewValue> value(NewValue value) {
      this.value = value;
      return (Builder<Key, NewValue>) this;
    }

    public Builder<Key, Value> topicPartition(TopicPartition topicPartition) {
      topic(topicPartition.topic());
      return partition(topicPartition.partition());
    }

    public Builder<Key, Value> topic(String topic) {
      this.topic = Objects.requireNonNull(topic);
      return this;
    }

    public Builder<Key, Value> partition(int partition) {
      if (partition >= 0) this.partition = partition;
      return this;
    }

    public Builder<Key, Value> timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder<Key, Value> headers(List<Header> headers) {
      this.headers = headers;
      return this;
    }

    public Builder<Key, Value> serializedKeySize(int serializedKeySize) {
      this.serializedKeySize = serializedKeySize;
      return this;
    }

    public Builder<Key, Value> serializedValueSize(int serializedValueSize) {
      this.serializedValueSize = serializedValueSize;
      return this;
    }

    public Builder<Key, Value> leaderEpoch(Optional<Integer> leaderEpoch) {
      this.leaderEpoch = leaderEpoch;
      return this;
    }

    public Builder<Key, Value> offset(long offset) {
      this.offset = offset;
      return this;
    }

    @SuppressWarnings("unchecked")
    public Record<Key, Value> build() {
      return new Record<>() {
        private final Key key = (Key) Builder.this.key;
        private final Value value = (Value) Builder.this.value;
        private final String topic = Objects.requireNonNull(Builder.this.topic);

        private final long offset = Builder.this.offset;
        private final int partition = Builder.this.partition;
        private final long timestamp = Builder.this.timestamp;
        private final List<Header> headers = Objects.requireNonNull(Builder.this.headers);

        private final int serializedKeySize = Builder.this.serializedKeySize;
        private final int serializedValueSize = Builder.this.serializedValueSize;

        private final Optional<Integer> leaderEpoch =
            Objects.requireNonNull(Builder.this.leaderEpoch);

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
}
