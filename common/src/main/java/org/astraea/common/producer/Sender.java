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

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import org.astraea.common.consumer.Header;

public interface Sender<Key, Value> {
  Sender<Key, Value> key(Key key);

  Sender<Key, Value> value(Value value);

  Sender<Key, Value> topic(String topic);

  /**
   * define the data route if you don't want to partitioner to decide the target.
   *
   * @param partition target partition. negative value is ignored
   * @return this sender
   */
  Sender<Key, Value> partition(int partition);

  default Sender<Key, Value> topicPartition(TopicPartition topicPartition) {
    topic(topicPartition.topic());
    partition(topicPartition.partition());
    return this;
  }

  Sender<Key, Value> timestamp(long timestamp);

  Sender<Key, Value> headers(Collection<Header> headers);

  /**
   * send data to servers. This operation is running in background. You have to call {@link
   * CompletionStage#toCompletableFuture()} to wait response of servers.
   *
   * @return an async operation stage.
   */
  CompletionStage<Metadata> run();
}
