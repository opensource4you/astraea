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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.astraea.common.FixedIterable;
import org.astraea.common.Header;
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

  /**
   * build a record stream by invoking a background running thread. The thread will close the
   * consumer when closeFlag returns true;
   *
   * @param limits to control the size of returned iterator
   * @return stream with records
   */
  public Iterator<Record<Key, Value>> iterator(List<IteratorLimit<Key, Value>> limits) {
    var queue = new LinkedBlockingQueue<FixedIterable<Record<Key, Value>>>();
    var exception = new AtomicReference<RuntimeException>();
    var f =
        CompletableFuture.runAsync(
            () -> {
              try (var consumer = build()) {
                while (true) {
                  var records = consumer.poll(Duration.ofSeconds(1));
                  if (records.nonEmpty()) queue.add(records);
                  if (limits.stream().anyMatch(l -> l.done(records))) return;
                }
              } catch (RuntimeException e) {
                exception.set(e);
              } finally {
                queue.add(FixedIterable.empty());
              }
            });

    return new Iterator<>() {

      private Iterator<Record<Key, Value>> current;

      private boolean done = false;

      @Override
      public boolean hasNext() {
        if (done) return false;
        if (current != null && current.hasNext()) return true;
        if (exception.get() != null) throw exception.get();
        var next = Utils.packException(queue::take);
        if (next.isEmpty()) {
          done = true;
          return false;
        }
        current = next.iterator();
        return current.hasNext();
      }

      @Override
      public Record<Key, Value> next() {
        if (!hasNext()) throw new NoSuchElementException("there is no more record");
        return current.next();
      }
    };
  }

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
    public FixedIterable<Record<Key, Value>> poll(Duration timeout) {
      final var end = System.currentTimeMillis() + timeout.toMillis();
      do {
        var records =
            kafkaConsumer.poll(Duration.ofMillis(Math.max(0, end - System.currentTimeMillis())));
        if (!records.isEmpty())
          return FixedIterable.of(
              records.count(),
              () ->
                  new Iterator<>() {
                    private final Iterator<ConsumerRecord<Key, Value>> iter = records.iterator();

                    @Override
                    public boolean hasNext() {
                      return iter.hasNext();
                    }

                    @Override
                    public Record<Key, Value> next() {
                      return toRecord(iter.next());
                    }
                  });
      } while (end > System.currentTimeMillis());
      return FixedIterable.empty();
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

  private static <Key, Value> Record<Key, Value> toRecord(ConsumerRecord<Key, Value> record) {
    return Record.builder()
        .topic(record.topic())
        .headers(Header.of(record.headers()))
        .key(record.key())
        .value(record.value())
        .offset(record.offset())
        .timestamp(record.timestamp())
        .partition(record.partition())
        .serializedKeySize(record.serializedKeySize())
        .serializedValueSize(record.serializedValueSize())
        .leaderEpoch(record.leaderEpoch())
        .build();
  }
}
