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
package org.astraea.app.consumer;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class TopicsBuilder<Key, Value> extends Builder<Key, Value> {
  private final Set<String> topics;
  private ConsumerRebalanceListener listener = ignore -> {};

  TopicsBuilder(Set<String> topics) {
    this.topics = requireNonNull(topics);
    config(ConsumerConfig.GROUP_ID_CONFIG, "groupId-" + System.currentTimeMillis());
  }

  public TopicsBuilder<Key, Value> groupId(String groupId) {
    config(ConsumerConfig.GROUP_ID_CONFIG, requireNonNull(groupId));
    return this;
  }

  public TopicsBuilder<Key, Value> consumerRebalanceListener(ConsumerRebalanceListener listener) {
    this.listener = Objects.requireNonNull(listener);
    return this;
  }

  /**
   * make the consumer read data from beginning. By default, it reads the latest data.
   *
   * @return this builder
   */
  @Override
  public TopicsBuilder<Key, Value> fromBeginning() {
    super.fromBeginning();
    return this;
  }

  /**
   * make the consumer read data from latest. this is default setting.
   *
   * @return this builder
   */
  @Override
  public TopicsBuilder<Key, Value> fromLatest() {
    super.fromLatest();
    return this;
  }

  /**
   * set the offset to read from the latest offset. For example, the end offset is 5, and you set
   * distanceFromLatest to 2, then you will read data from offset: 3
   *
   * @param distanceFromLatest the distance from the latest offset
   * @return this builder
   */
  @Override
  public TopicsBuilder<Key, Value> distanceFromLatest(int distanceFromLatest) {
    super.distanceFromLatest(distanceFromLatest);
    return this;
  }

  @Override
  public <NewKey> TopicsBuilder<NewKey, Value> keyDeserializer(
      Deserializer<NewKey> keyDeserializer) {
    return (TopicsBuilder<NewKey, Value>) super.keyDeserializer(keyDeserializer);
  }

  @Override
  public <NewValue> TopicsBuilder<Key, NewValue> valueDeserializer(
      Deserializer<NewValue> valueDeserializer) {
    return (TopicsBuilder<Key, NewValue>) super.valueDeserializer(valueDeserializer);
  }

  @Override
  public TopicsBuilder<Key, Value> config(String key, String value) {
    super.config(key, value);
    return this;
  }

  @Override
  public TopicsBuilder<Key, Value> configs(Map<String, String> configs) {
    super.configs(configs);
    return this;
  }

  @Override
  public TopicsBuilder<Key, Value> bootstrapServers(String bootstrapServers) {
    super.bootstrapServers(bootstrapServers);
    return this;
  }

  @Override
  public TopicsBuilder<Key, Value> isolation(Isolation isolation) {
    super.isolation(isolation);
    return this;
  }

  @Override
  protected void assignOrSubscribe(Consumer<Key, Value> kafkaConsumer) {
    kafkaConsumer.subscribe(topics, ConsumerRebalanceListener.of(listener));
  }
}
