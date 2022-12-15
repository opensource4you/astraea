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
import java.util.Optional;
import org.astraea.common.Header;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.producer.Record;

public class SourceRecord implements Record<byte[], byte[]> {

  public static Builder builder() {
    return new Builder();
  }

  private final Record<byte[], byte[]> record;
  private final Map<String, String> metadataIndex;
  private final Map<String, String> metadata;

  private SourceRecord(
      Record<byte[], byte[]> record,
      Map<String, String> metadataIndex,
      Map<String, String> metadata) {
    this.record = record;
    this.metadataIndex = metadataIndex;
    this.metadata = metadata;
  }

  @Override
  public String topic() {
    return record.topic();
  }

  @Override
  public List<Header> headers() {
    return record.headers();
  }

  @Override
  public byte[] key() {
    return record.key();
  }

  @Override
  public byte[] value() {
    return record.value();
  }

  @Override
  public Optional<Long> timestamp() {
    return record.timestamp();
  }

  @Override
  public Optional<Integer> partition() {
    return record.partition();
  }

  public Map<String, String> metadataIndex() {
    return metadataIndex;
  }

  public Map<String, String> metadata() {
    return metadata;
  }

  public static class Builder {

    private final Record.Builder<byte[], byte[]> builder = Record.builder();
    private Map<String, String> metadataIndex = Map.of();
    private Map<String, String> metadata = Map.of();

    private Builder() {}

    public Builder record(Record<byte[], byte[]> record) {
      builder.record(record);
      return this;
    }

    public Builder key(byte[] key) {
      builder.key(key);
      return this;
    }

    public Builder value(byte[] value) {
      builder.value(value);
      return this;
    }

    public Builder topicPartition(TopicPartition topicPartition) {
      topic(topicPartition.topic());
      return partition(topicPartition.partition());
    }

    public Builder topic(String topic) {
      builder.topic(topic);
      return this;
    }

    public Builder partition(int partition) {
      builder.partition(partition);
      return this;
    }

    public Builder timestamp(long timestamp) {
      builder.timestamp(timestamp);
      return this;
    }

    public Builder headers(List<Header> headers) {
      builder.headers(headers);
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
      return new SourceRecord(builder.build(), metadataIndex, metadata);
    }
  }
}
