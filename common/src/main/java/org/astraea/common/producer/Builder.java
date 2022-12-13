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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.astraea.common.Utils;

public class Builder<Key, Value> {
  private final Map<String, Object> configs = new HashMap<>();
  private Serializer<?> keySerializer = Serializer.BYTE_ARRAY;
  private Serializer<?> valueSerializer = Serializer.BYTE_ARRAY;

  Builder() {}

  @SuppressWarnings("unchecked")
  public <NewKey> Builder<NewKey, Value> keySerializer(Serializer<NewKey> keySerializer) {
    this.keySerializer = Objects.requireNonNull(keySerializer);
    return (Builder<NewKey, Value>) this;
  }

  @SuppressWarnings("unchecked")
  public <NewValue> Builder<Key, NewValue> valueSerializer(Serializer<NewValue> valueSerializer) {
    this.valueSerializer = Objects.requireNonNull(valueSerializer);
    return (Builder<Key, NewValue>) this;
  }

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
    configs.forEach(this::config);
    return this;
  }

  public Builder<Key, Value> bootstrapServers(String bootstrapServers) {
    return config(
        ProducerConfigs.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers));
  }

  private static <Key, Value> CompletionStage<Metadata> doSend(
      org.apache.kafka.clients.producer.Producer<Key, Value> producer, Record<Key, Value> record) {

    var completableFuture = new CompletableFuture<Metadata>();
    producer.send(
        new ProducerRecord<>(
            record.topic(),
            record.partition().orElse(null),
            record.timestamp().orElse(null),
            record.key(),
            record.value(),
            record.headers().stream()
                .map(
                    e ->
                        new org.apache.kafka.common.header.Header() {

                          @Override
                          public String key() {
                            return e.key();
                          }

                          @Override
                          public byte[] value() {
                            return e.value();
                          }
                        })
                .collect(Collectors.toList())),
        (metadata, exception) -> {
          if (exception == null) completableFuture.completeAsync(() -> Metadata.of(metadata));
          else completableFuture.completeExceptionally(exception);
        });
    return completableFuture;
  }

  /**
   * @return normal producer if there is no transaction id in configs. Otherwise, a transactional
   *     producer is returned
   */
  @SuppressWarnings("unchecked")
  public Producer<Key, Value> build() {
    // if user configs the transaction id, we should build transactional producer
    if (configs.containsKey(ProducerConfigs.TRANSACTIONAL_ID_CONFIG)) return buildTransactional();
    return new NormalProducer<>(
        new KafkaProducer<>(
            configs,
            Serializer.of((Serializer<Key>) keySerializer),
            Serializer.of((Serializer<Value>) valueSerializer)));
  }

  /**
   * @return always returns a transactional producer. The transaction id is generated automatically
   *     if it is absent
   */
  @SuppressWarnings("unchecked")
  public Producer<Key, Value> buildTransactional() {
    var transactionConfigs = new HashMap<>(configs);
    var transactionId =
        (String)
            transactionConfigs.computeIfAbsent(
                ProducerConfigs.TRANSACTIONAL_ID_CONFIG,
                ignored -> "transaction-id-" + new Random().nextLong());
    // For transactional send
    var transactionProducer =
        new KafkaProducer<>(
            transactionConfigs,
            Serializer.of((Serializer<Key>) keySerializer),
            Serializer.of((Serializer<Value>) valueSerializer));
    transactionProducer.initTransactions();
    return new TransactionalProducer<>(transactionProducer, transactionId);
  }

  private abstract static class BaseProducer<Key, Value> implements Producer<Key, Value> {
    protected final org.apache.kafka.clients.producer.Producer<Key, Value> kafkaProducer;
    private final String clientId;

    private BaseProducer(org.apache.kafka.clients.producer.Producer<Key, Value> kafkaProducer) {
      this.kafkaProducer = kafkaProducer;
      // KafkaConsumer does not expose client-id
      this.clientId = (String) Utils.member(kafkaProducer, "clientId");
    }

    @Override
    public void flush() {
      kafkaProducer.flush();
    }

    @Override
    public void close() {
      kafkaProducer.close();
    }

    @Override
    public String clientId() {
      return clientId;
    }
  }

  private static class NormalProducer<Key, Value> extends BaseProducer<Key, Value> {
    private NormalProducer(org.apache.kafka.clients.producer.Producer<Key, Value> kafkaProducer) {
      super(kafkaProducer);
    }

    @Override
    public CompletionStage<Metadata> send(Record<Key, Value> record) {
      return doSend(kafkaProducer, record);
    }

    @Override
    public Collection<CompletionStage<Metadata>> send(Collection<Record<Key, Value>> records) {
      return records.stream().map(this::send).collect(Collectors.toUnmodifiableList());
    }

    @Override
    public Optional<String> transactionId() {
      return Optional.empty();
    }
  }

  private static class TransactionalProducer<Key, Value> extends BaseProducer<Key, Value> {
    private final String transactionId;

    private TransactionalProducer(
        org.apache.kafka.clients.producer.Producer<Key, Value> kafkaProducer,
        String transactionId) {
      super(kafkaProducer);
      this.transactionId = transactionId;
    }

    @Override
    public CompletionStage<Metadata> send(Record<Key, Value> record) {
      return send(List.of(record)).iterator().next();
    }

    @Override
    public Collection<CompletionStage<Metadata>> send(Collection<Record<Key, Value>> records) {
      try {
        kafkaProducer.beginTransaction();
        var futures =
            records.stream()
                .map(r -> doSend(kafkaProducer, r))
                .collect(Collectors.toUnmodifiableList());
        kafkaProducer.commitTransaction();
        return futures;
      } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
        kafkaProducer.close();
        // Error occur
        throw e;
      } catch (KafkaException ke) {
        kafkaProducer.abortTransaction();
        return send(records);
      }
    }

    @Override
    public boolean transactional() {
      return true;
    }

    @Override
    public Optional<String> transactionId() {
      return Optional.of(transactionId);
    }
  }
}
