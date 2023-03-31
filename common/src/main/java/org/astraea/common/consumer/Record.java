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
import java.util.Optional;
import org.astraea.common.Header;
import org.astraea.common.admin.TopicPartition;

public interface Record<Key, Value> {

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
}
