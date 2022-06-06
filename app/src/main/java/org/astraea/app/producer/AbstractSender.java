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
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.astraea.app.consumer.Header;

abstract class AbstractSender<Key, Value> implements Sender<Key, Value> {
  protected Key key;
  protected Value value;
  protected String topic;
  protected Integer partition;
  protected Long timestamp;
  protected Collection<Header> headers = List.of();

  @Override
  public Sender<Key, Value> key(Key key) {
    this.key = key;
    return this;
  }

  @Override
  public Sender<Key, Value> value(Value value) {
    this.value = value;
    return this;
  }

  @Override
  public Sender<Key, Value> topic(String topic) {
    this.topic = Objects.requireNonNull(topic);
    return this;
  }

  @Override
  public Sender<Key, Value> partition(int partition) {
    if (partition >= 0) this.partition = partition;
    return this;
  }

  @Override
  public Sender<Key, Value> timestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  @Override
  public Sender<Key, Value> headers(Collection<Header> headers) {
    this.headers = headers;
    return this;
  }

  /**
   * this helper method is used by our producer only.
   *
   * @return a kafka producer record
   */
  ProducerRecord<Key, Value> record() {
    return new ProducerRecord<>(topic, partition, timestamp, key, value, Header.of(headers));
  }
}
