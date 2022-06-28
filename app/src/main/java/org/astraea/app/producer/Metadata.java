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
  public int serializedKeySize() {
    return serializedKeySize;
  }

  /**
   * @return The size of the serialized, uncompressed value in bytes. If value is null, the returned
   *     size is -1.
   */
  public int serializedValueSize() {
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
