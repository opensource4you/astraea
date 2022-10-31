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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;

public abstract class Builder<Key, Value> {
  protected final Map<String, Object> configs = new HashMap<>();
  protected Deserializer<?> keyDeserializer = Deserializer.BYTE_ARRAY;
  protected Deserializer<?> valueDeserializer = Deserializer.BYTE_ARRAY;

  protected SeekStrategy seekStrategy = SeekStrategy.NONE;
  protected Object seekValue = null;

  Builder() {}

  /**
   * @param key a non-null string
   * @param value null means you want to remove the associated key. Otherwise, the previous value
   *     will be overwritten
   * @return this builder
   */
  public Builder<Key, Value> config(String key, String value) {
    if (value == null) this.configs.remove(key);
    else this.configs.put(key, value);
    return this;
  }

  public Builder<Key, Value> configs(Map<String, String> configs) {
    this.configs.putAll(configs);
    return this;
  }

  /**
   * Set seek strategy name and its accepted value, seek strategies are as follows:
   * <li>{@link SeekStrategy#DISTANCE_FROM_LATEST DISTANCE_FROM_LATEST}: set the offset to read from
   *     the latest offset. For example, the end offset is 5, and you set {@code value} to 2,then
   *     you will read data from offset: 3
   * <li>{@link SeekStrategy#DISTANCE_FROM_BEGINNING DISTANCE_FROM_BEGINNING}: set the offset to
   *     read from the beginning offset. For example, the beginning offset is 0, and you set {@code
   *     value} to 2, then you will read data from offset: 2
   * <li>{@link SeekStrategy#SEEK_TO SEEK_TO}: set the offset to read from. For example, the {@code
   *     value} is 15, then you will read data from offset: 15
   *
   * @param seekStrategy seek strategy
   * @param value the value that will send to seek strategy, if value < 0, throw {@code
   *     IllegalArgumentException}
   * @return this builder
   */
  public Builder<Key, Value> seek(SeekStrategy seekStrategy, long value) {
    this.seekStrategy = requireNonNull(seekStrategy);
    if (value < 0) {
      throw new IllegalArgumentException("seek value should >= 0");
    }
    this.seekValue = value;
    return this;
  }

  public Builder<Key, Value> seek(Map<TopicPartition, Long> offsets) {
    this.seekStrategy = SeekStrategy.SEEK_TO;
    this.seekValue = offsets;
    return this;
  }

  @SuppressWarnings("unchecked")
  public <NewKey> Builder<NewKey, Value> keyDeserializer(Deserializer<NewKey> keyDeserializer) {
    this.keyDeserializer = requireNonNull(keyDeserializer);
    return (Builder<NewKey, Value>) this;
  }

  @SuppressWarnings("unchecked")
  public <NewValue> Builder<Key, NewValue> valueDeserializer(
      Deserializer<NewValue> valueDeserializer) {
    this.valueDeserializer = requireNonNull(valueDeserializer);
    return (Builder<Key, NewValue>) this;
  }

  public Builder<Key, Value> bootstrapServers(String bootstrapServers) {
    this.configs.put(ConsumerConfigs.BOOTSTRAP_SERVERS_CONFIG, requireNonNull(bootstrapServers));
    return this;
  }

  /**
   * @return consumer instance. The different builders may return inherited consumer interface.
   */
  public abstract Consumer<Key, Value> build();

  protected abstract static class BaseConsumer<Key, Value> implements Consumer<Key, Value> {
    protected final org.apache.kafka.clients.consumer.Consumer<Key, Value> kafkaConsumer;
    private final AtomicBoolean subscribed = new AtomicBoolean(true);

    private final String clientId;

    public BaseConsumer(org.apache.kafka.clients.consumer.Consumer<Key, Value> kafkaConsumer) {
      this.kafkaConsumer = kafkaConsumer;
      // KafkaConsumer does not expose client-id
      this.clientId = (String) Utils.member(kafkaConsumer, "clientId");
    }

    @Override
    public Collection<Record<Key, Value>> poll(int recordCount, Duration timeout) {
      var end = System.currentTimeMillis() + timeout.toMillis();
      var records = new ArrayList<Record<Key, Value>>();
      while (records.size() < recordCount) {
        var remaining = end - System.currentTimeMillis();
        if (remaining <= 0) break;
        kafkaConsumer.poll(Duration.ofMillis(remaining)).forEach(r -> records.add(Record.of(r)));
      }
      return Collections.unmodifiableList(records);
    }

    @Override
    public void wakeup() {
      kafkaConsumer.wakeup();
    }

    @Override
    public void close() {
      kafkaConsumer.close();
    }

    @Override
    public void resubscribe() {
      if (subscribed.compareAndSet(false, true)) doResubscribe();
    }

    @Override
    public void unsubscribe() {
      if (subscribed.compareAndSet(true, false)) kafkaConsumer.unsubscribe();
    }

    @Override
    public String clientId() {
      return clientId;
    }

    protected abstract void doResubscribe();
  }
}
