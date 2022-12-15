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
package org.astraea.common.producer;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.astraea.common.Header;
import org.astraea.common.admin.TopicPartition;

public interface Record<Key, Value> {

  static Builder<byte[], byte[]> builder() {
    return new Builder<>();
  }

  String topic();

  List<Header> headers();

  Key key();

  Value value();

  /**
   * @return timestamp of record
   */
  Optional<Long> timestamp();

  /**
   * @return expected partition, or null if you don't care for it.
   */
  Optional<Integer> partition();

  class Builder<Key, Value> {
    private Object key;
    private Object value;
    private String topic;
    private Optional<Integer> partition = Optional.empty();
    private Optional<Long> timestamp = Optional.empty();
    private List<Header> headers = List.of();

    private Builder() {}

    @SuppressWarnings("unchecked")
    public <NewKey, NewValue> Builder<NewKey, NewValue> record(Record<NewKey, NewValue> record) {
      key(record.key());
      value(record.value());
      topic(record.topic());
      record.partition().ifPresent(this::partition);
      record.timestamp().ifPresent(this::timestamp);
      headers(record.headers());
      return (Builder<NewKey, NewValue>) this;
    }

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
      if (partition >= 0) this.partition = Optional.of(partition);
      return this;
    }

    public Builder<Key, Value> timestamp(long timestamp) {
      this.timestamp = Optional.of(timestamp);
      return this;
    }

    public Builder<Key, Value> headers(List<Header> headers) {
      this.headers = headers;
      return this;
    }

    @SuppressWarnings("unchecked")
    public Record<Key, Value> build() {
      return new Record<>() {
        private final Key key = (Key) Builder.this.key;
        private final Value value = (Value) Builder.this.value;
        private final String topic = Objects.requireNonNull(Builder.this.topic);
        private final Optional<Integer> partition = Builder.this.partition;
        private final Optional<Long> timestamp = Builder.this.timestamp;
        private final List<Header> headers = Objects.requireNonNull(Builder.this.headers);

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
        public Optional<Long> timestamp() {
          return timestamp;
        }

        @Override
        public Optional<Integer> partition() {
          return partition;
        }
      };
    }
  }
}
