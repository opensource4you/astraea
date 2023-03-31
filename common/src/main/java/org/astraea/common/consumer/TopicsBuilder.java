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

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.astraea.common.FixedIterable;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;

public class TopicsBuilder<Key, Value> extends Builder<Key, Value> {
  private final Set<String> setTopics;
  private final Pattern patternTopics;
  private ConsumerRebalanceListener listener = ignore -> {};

  TopicsBuilder(Set<String> topics) {
    this.patternTopics = null;
    this.setTopics = requireNonNull(topics);
  }

  TopicsBuilder(Pattern patternTopics) {
    this.setTopics = null;
    this.patternTopics = patternTopics;
  }

  public TopicsBuilder<Key, Value> consumerRebalanceListener(ConsumerRebalanceListener listener) {
    this.listener = Objects.requireNonNull(listener);
    return this;
  }

  @Override
  public TopicsBuilder<Key, Value> seek(SeekStrategy seekStrategy, long value) {
    super.seek(seekStrategy, value);
    return this;
  }

  @Override
  public TopicsBuilder<Key, Value> seek(Map<TopicPartition, Long> offsets) {
    super.seek(offsets);
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
    super.config(key, value);
    return this;
  }

  public TopicsBuilder<Key, Value> configs(Map<String, String> configs) {
    super.configs(configs);
    return this;
  }

  @Override
  public TopicsBuilder<Key, Value> bootstrapServers(String bootstrapServers) {
    super.bootstrapServers(bootstrapServers);
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SubscribedConsumer<Key, Value> build() {
    // generate group id if it is empty
    configs.putIfAbsent(ConsumerConfigs.GROUP_ID_CONFIG, "groupId-" + System.currentTimeMillis());

    var kafkaConsumer =
        new KafkaConsumer<>(
            configs,
            Deserializer.of((Deserializer<Key>) keyDeserializer),
            Deserializer.of((Deserializer<Value>) valueDeserializer));

    // consumer rebalance requires all consumers keeps polling to complete whole process.
    // we invoke another thread to poll consumer to complete rebalance
    // otherwise, users can't create multi consumers at once in same thread.
    CompletableFuture<Void> waitRebalance = CompletableFuture.completedFuture(null);
    if (seekStrategy != SeekStrategy.NONE) {
      // make sure this consumer is assigned before seeking
      var latch = new CountDownLatch(1);
      if (patternTopics == null)
        kafkaConsumer.subscribe(
            setTopics,
            ConsumerRebalanceListener.of(List.of(listener, ignored -> latch.countDown())));
      else
        kafkaConsumer.subscribe(
            patternTopics,
            ConsumerRebalanceListener.of(List.of(listener, ignored -> latch.countDown())));

      waitRebalance =
          CompletableFuture.runAsync(
              () -> {
                while (latch.getCount() != 0) {
                  // the offset will be reset, so it is fine to poll data
                  // TODO: should we disable auto-commit here?
                  kafkaConsumer.poll(Duration.ofMillis(500));
                  Utils.sleep(Duration.ofSeconds(1));
                }
                seekStrategy.apply(kafkaConsumer, seekValue);
              });
    } else {
      // nothing to seek so we just subscribe topics
      if (patternTopics == null)
        kafkaConsumer.subscribe(setTopics, ConsumerRebalanceListener.of(List.of(listener)));
      else kafkaConsumer.subscribe(patternTopics, ConsumerRebalanceListener.of(List.of(listener)));
    }

    return new SubscribedConsumerImpl<>(
        waitRebalance, kafkaConsumer, setTopics, patternTopics, listener);
  }

  private static class SubscribedConsumerImpl<Key, Value> extends Builder.BaseConsumer<Key, Value>
      implements SubscribedConsumer<Key, Value> {
    private final CompletableFuture<Void> waitRebalance;
    private final Set<String> setTopics;
    private final Pattern patternTopics;
    private final ConsumerRebalanceListener listener;

    public SubscribedConsumerImpl(
        CompletableFuture<Void> waitRebalance,
        org.apache.kafka.clients.consumer.Consumer<Key, Value> kafkaConsumer,
        Set<String> setTopics,
        Pattern patternTopics,
        ConsumerRebalanceListener listener) {
      super(kafkaConsumer);
      this.waitRebalance = waitRebalance;
      this.setTopics = setTopics;
      this.patternTopics = patternTopics;
      this.listener = listener;
    }

    @Override
    public void unsubscribe() {
      waitRebalance.join();
      super.unsubscribe();
    }

    @Override
    public FixedIterable<Record<Key, Value>> poll(Duration timeout) {
      waitRebalance.join();
      return super.poll(timeout);
    }

    @Override
    public void close() {
      waitRebalance.join();
      super.close();
    }

    @Override
    public void commitOffsets(Duration timeout) {
      waitRebalance.join();
      kafkaConsumer.commitSync(timeout);
    }

    @Override
    public String groupId() {
      waitRebalance.join();
      return kafkaConsumer.groupMetadata().groupId();
    }

    @Override
    public String memberId() {
      waitRebalance.join();
      return kafkaConsumer.groupMetadata().memberId();
    }

    public Optional<String> groupInstanceId() {
      waitRebalance.join();
      return kafkaConsumer.groupMetadata().groupInstanceId();
    }

    @Override
    protected void doResubscribe() {
      waitRebalance.join();
      if (patternTopics == null)
        kafkaConsumer.subscribe(setTopics, ConsumerRebalanceListener.of(List.of(listener)));
      else kafkaConsumer.subscribe(patternTopics, ConsumerRebalanceListener.of(List.of(listener)));
    }

    @Override
    public Set<TopicPartition> assignments() {
      waitRebalance.join();
      return kafkaConsumer.assignment().stream()
          .map(TopicPartition::from)
          .collect(Collectors.toUnmodifiableSet());
    }
  }
}
