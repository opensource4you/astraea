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

import java.util.Optional;
import org.apache.kafka.clients.producer.RecordMetadata;

public interface Metadata {

  static Metadata of(RecordMetadata metadata) {
    return new Metadata() {
      @Override
      public long offset() {
        return metadata.offset();
      }

      @Override
      public int serializedKeySize() {
        return metadata.serializedKeySize();
      }

      @Override
      public int serializedValueSize() {
        return metadata.serializedValueSize();
      }

      @Override
      public Optional<Long> timestamp() {
        return metadata.hasTimestamp() ? Optional.of(metadata.timestamp()) : Optional.empty();
      }

      @Override
      public String topic() {
        return metadata.topic();
      }

      @Override
      public int partition() {
        return metadata.partition();
      }
    };
  }

  /**
   * The offset of the record in the topic/partition.
   *
   * @return the offset of the record, or -1
   */
  long offset();

  /**
   * @return The size of the serialized, uncompressed key in bytes. If key is null, the returned
   *     size is -1.
   */
  int serializedKeySize();

  /**
   * @return The size of the serialized, uncompressed value in bytes. If value is null, the returned
   *     size is -1.
   */
  int serializedValueSize();

  /**
   * @return the timestamp of the record
   */
  Optional<Long> timestamp();

  /**
   * @return The topic the record was appended to
   */
  String topic();

  /**
   * @return The partition the record was sent to
   */
  int partition();
}
