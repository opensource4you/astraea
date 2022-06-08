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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/** An interface for sending records. */
public interface Producer<Key, Value> extends AutoCloseable {
  Sender<Key, Value> sender();

  /**
   * send the multiple records. Noted that the normal producer will send the record one by one. By
   * contrast, transactional producer will send all records in single transaction.
   *
   * @param senders pre-defined records
   * @return callback of all completed records
   */
  Collection<CompletionStage<Metadata>> send(Collection<Sender<Key, Value>> senders);

  /** this method is blocked until all data in buffer are sent. */
  void flush();

  void close();

  /** @return true if the producer supports transactional. */
  boolean transactional();

  static Builder<byte[], byte[]> builder() {
    return new Builder<>();
  }

  static Producer<byte[], byte[]> of(String bootstrapServers) {
    return builder().bootstrapServers(bootstrapServers).build();
  }

  static Producer<byte[], byte[]> of(Map<String, String> configs) {
    return builder().configs(configs).build();
  }
}
