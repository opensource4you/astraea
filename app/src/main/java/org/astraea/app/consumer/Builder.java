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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerConfig;

abstract class Builder<Key, Value> {
  protected final Map<String, Object> configs = new HashMap<>();
  protected Deserializer<?> keyDeserializer = Deserializer.BYTE_ARRAY;
  protected Deserializer<?> valueDeserializer = Deserializer.BYTE_ARRAY;
  protected int distanceFromLatest = -1;

  Builder() {}

  /**
   * make the consumer read data from beginning. By default, it reads the latest data.
   *
   * @return this builder
   */
  public Builder<Key, Value> fromBeginning() {
    this.configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return this;
  }

  /**
   * make the consumer read data from latest. this is default setting.
   *
   * @return this builder
   */
  public Builder<Key, Value> fromLatest() {
    this.configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    return this;
  }

  /**
   * set the offset to read from the latest offset. For example, the end offset is 5, and you set
   * distanceFromLatest to 2, then you will read data from offset: 3
   *
   * @param distanceFromLatest the distance from the latest offset
   * @return this builder
   */
  public Builder<Key, Value> distanceFromLatest(int distanceFromLatest) {
    this.distanceFromLatest = distanceFromLatest;
    return this;
  }

  @SuppressWarnings("unchecked")
  public <NewKey> Builder<NewKey, Value> keyDeserializer(Deserializer<NewKey> keyDeserializer) {
    this.keyDeserializer = Objects.requireNonNull(keyDeserializer);
    return (Builder<NewKey, Value>) this;
  }

  @SuppressWarnings("unchecked")
  public <NewValue> Builder<Key, NewValue> valueDeserializer(
      Deserializer<NewValue> valueDeserializer) {
    this.valueDeserializer = Objects.requireNonNull(valueDeserializer);
    return (Builder<Key, NewValue>) this;
  }

  public Builder<Key, Value> bootstrapServers(String bootstrapServers) {
    this.configs.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers));
    return this;
  }

  public Builder<Key, Value> isolation(Isolation isolation) {
    this.configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation.nameOfKafka());
    return this;
  }

  /** @return consumer instance. The different builders may return inherited consumer interface. */
  public abstract Consumer<Key, Value> build();

  protected static class BaseConsumer<Key, Value> implements Consumer<Key, Value> {
    protected final org.apache.kafka.clients.consumer.Consumer<Key, Value> kafkaConsumer;

    public BaseConsumer(org.apache.kafka.clients.consumer.Consumer<Key, Value> kafkaConsumer) {
      this.kafkaConsumer = kafkaConsumer;
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
  }
}
