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
package org.astraea.connector;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.astraea.common.Header;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.producer.Record;

/**
 * @param timestamp timestamp of record
 * @param partition expected partition, or null if you don't care for it.
 */
public record SourceRecord(
    String topic,
    List<Header> headers,
    byte[] key,
    byte[] value,
    Optional<Long> timestamp,
    Optional<Integer> partition,
    Map<String, String> metadataIndex,
    Map<String, String> metadata) {

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private byte[] key;
    private byte[] value;
    private String topic;
    private Optional<Integer> partition = Optional.empty();
    private Optional<Long> timestamp = Optional.empty();
    private List<Header> headers = List.of();
    private Map<String, String> metadataIndex = Map.of();
    private Map<String, String> metadata = Map.of();

    private Builder() {}

    public Builder record(Record<byte[], byte[]> record) {
      key(record.key());
      value(record.value());
      topic(record.topic());
      record.partition().ifPresent(this::partition);
      record.timestamp().ifPresent(this::timestamp);
      headers(record.headers());
      return this;
    }

    public Builder key(byte[] key) {
      this.key = key;
      return this;
    }

    public Builder value(byte[] value) {
      this.value = value;
      return this;
    }

    public Builder topicPartition(TopicPartition topicPartition) {
      topic(topicPartition.topic());
      return partition(topicPartition.partition());
    }

    public Builder topic(String topic) {
      this.topic = Objects.requireNonNull(topic);
      return this;
    }

    public Builder partition(int partition) {
      if (partition >= 0) this.partition = Optional.of(partition);
      return this;
    }

    public Builder timestamp(long timestamp) {
      this.timestamp = Optional.of(timestamp);
      return this;
    }

    public Builder headers(List<Header> headers) {
      this.headers = headers;
      return this;
    }

    public Builder metadataIndex(Map<String, String> metadataIndex) {
      this.metadataIndex = metadataIndex;
      return this;
    }

    public Builder metadata(Map<String, String> metadata) {
      this.metadata = metadata;
      return this;
    }

    public SourceRecord build() {
      return new SourceRecord(
          Objects.requireNonNull(topic, "topic must be defined"),
          Objects.requireNonNull(headers),
          key,
          value,
          timestamp,
          partition,
          metadataIndex,
          metadata);
    }
  }
}
