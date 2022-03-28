package org.astraea.producer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.astraea.consumer.Header;

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

  public Builder<Key, Value> configs(Map<String, Object> configs) {
    this.configs.putAll(configs);
    return this;
  }

  public Builder<Key, Value> brokers(String brokers) {
    this.configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(brokers));
    return this;
  }

  public Builder<Key, Value> partitionClassName(String partitionClassName) {
    this.configs.put(
        ProducerConfig.PARTITIONER_CLASS_CONFIG, Objects.requireNonNull(partitionClassName));
    return this;
  }

  @SuppressWarnings("unchecked")
  public Producer<Key, Value> build() {
    var kafkaProducer =
        new KafkaProducer<>(
            configs,
            Serializer.of((Serializer<Key>) keySerializer),
            Serializer.of((Serializer<Value>) valueSerializer));
    return new Producer<>() {

      @Override
      public Sender<Key, Value> sender() {
        return new AbstractSender<>() {
          @Override
          public CompletionStage<Metadata> run() {
            var completableFuture = new CompletableFuture<Metadata>();
            kafkaProducer.send(
                new ProducerRecord<>(topic, partition, timestamp, key, value, Header.of(headers)),
                (metadata, exception) -> {
                  if (exception == null) completableFuture.complete(Metadata.of(metadata));
                  else completableFuture.completeExceptionally(exception);
                });
            return completableFuture;
          }
        };
      }

      @Override
      public void flush() {
        kafkaProducer.flush();
      }

      @Override
      public KafkaProducer<Key, Value> kafkaProducer() {
        return kafkaProducer;
      }

      @Override
      public void close() {
        kafkaProducer.close();
      }
    };
  }

  @SuppressWarnings("unchecked")
  public TransactionalProducer<Key, Value> buildTransactional() {
    var transactionConfigs = new HashMap<>(configs);
    transactionConfigs.putIfAbsent(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG, "id" + new Random().nextLong());
    // For transactional send
    var transactionProducer =
        new KafkaProducer<>(
            transactionConfigs,
            Serializer.of((Serializer<Key>) keySerializer),
            Serializer.of((Serializer<Value>) valueSerializer));
    transactionProducer.initTransactions();
    return new TransactionalProducer<>() {

      @Override
      public Sender<Key, Value> sender() {
        return new TransactionalSender<>() {
          /** Send one transactional record. */
          @Override
          public CompletionStage<Metadata> run() {
            return transaction(List.of(this)).stream().findFirst().orElseThrow();
          }

          @Override
          CompletionStage<Metadata> send() {
            var completableFuture = new CompletableFuture<Metadata>();
            transactionProducer.send(
                new ProducerRecord<>(topic, partition, timestamp, key, value, Header.of(headers)),
                (metadata, exception) -> {
                  if (exception == null) completableFuture.complete(Metadata.of(metadata));
                  else completableFuture.completeExceptionally(exception);
                });
            return completableFuture;
          }
        };
      }

      @Override
      public KafkaProducer<Key, Value> kafkaProducer() {
        return transactionProducer;
      }

      @Override
      public void flush() {
        transactionProducer.flush();
      }

      @Override
      public void close() {
        transactionProducer.close();
      }

      /**
       * Send a collection of records as a transaction operation. For example,
       *
       * <pre>{@code
       * try(var producer = Producer.builder().brokers("localhost:9092").buildTransactional()){
       *     producer.transaction(
       *             IntStream.range(0, 10)
       *                     .mapToObj(i -> producer.sender().topic("topic1").value(new byte[10]))
       *                     .collect(Collectors.toList()));
       * }
       * }</pre>
       */
      @Override
      public Collection<CompletionStage<Metadata>> transaction(
          Collection<Sender<Key, Value>> senders) {
        var futures = new ArrayList<CompletionStage<Metadata>>(senders.size());
        try {
          synchronized (transactionProducer) {
            transactionProducer.beginTransaction();
            senders.forEach(
                sender -> futures.add(((TransactionalSender<Key, Value>) sender).send()));
            transactionProducer.commitTransaction();
          }
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
          transactionProducer.close();
          // Error occur
          throw e;
        } catch (KafkaException ke) {
          transactionProducer.abortTransaction();
        }
        return futures;
      }
    };
  }
}
