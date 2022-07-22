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

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TopicsBuilder<Key, Value> extends Builder<Key, Value> {
  private final Set<String> topics;
  private ConsumerRebalanceListener listener = ignore -> {};

  TopicsBuilder(Set<String> topics) {
    this.topics = requireNonNull(topics);
  }

  public TopicsBuilder<Key, Value> groupId(String groupId) {
    return config(ConsumerConfig.GROUP_ID_CONFIG, requireNonNull(groupId));
  }

  public TopicsBuilder<Key, Value> groupInstanceId(String groupInstanceId) {
    return config(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, requireNonNull(groupInstanceId));
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

  public TopicsBuilder<Key, Value> config(String key, String value) {
    this.configs.put(key, value);
    return this;
  }

  public TopicsBuilder<Key, Value> configs(Map<String, String> configs) {
    this.configs.putAll(configs);
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

  public TopicsBuilder<Key, Value> disableAutoCommitOffsets() {
    return config(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
  }

  @SuppressWarnings("unchecked")
  @Override
  public SubscribedConsumer<Key, Value> build() {
    // generate group id if it is empty
    configs.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "groupId-" + System.currentTimeMillis());

    var kafkaConsumer =
        new KafkaConsumer<>(
            configs,
            Deserializer.of((Deserializer<Key>) keyDeserializer),
            Deserializer.of((Deserializer<Value>) valueDeserializer));
    kafkaConsumer.subscribe(topics, ConsumerRebalanceListener.of(listener));

    seekStrategy.apply(kafkaConsumer, seekValue);

    return new SubscribedConsumerImpl<>(kafkaConsumer);
  }

  private static class SubscribedConsumerImpl<Key, Value> extends Builder.BaseConsumer<Key, Value>
      implements SubscribedConsumer<Key, Value> {

    public SubscribedConsumerImpl(
        org.apache.kafka.clients.consumer.Consumer<Key, Value> kafkaConsumer) {
      super(kafkaConsumer);
    }

    @Override
    public void commitOffsets(Duration timeout) {
      kafkaConsumer.commitSync(timeout);
    }

    @Override
    public String groupId() {
      return kafkaConsumer.groupMetadata().groupId();
    }

    @Override
    public String memberId() {
      return kafkaConsumer.groupMetadata().memberId();
    }

    public Optional<String> groupInstanceId() {
      return kafkaConsumer.groupMetadata().groupInstanceId();
    }
  }
}
