package org.astraea.producer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
    if (!configs.getOrDefault(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "").toString().isEmpty())
      kafkaProducer.initTransactions();
    return new Producer<>() {

      @Override
      public Sender<Key, Value> sender() {
        return new Sender<>() {
          private Key key;
          private Value value;
          private String topic;
          private Integer partition;
          private Long timestamp;
          private Collection<Header> headers = List.of();

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
            this.partition = partition;
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
      public Collection<CompletionStage<Metadata>> transaction(
          Collection<Sender<Key, Value>> senders) {
        var futures = new ArrayList<CompletionStage<Metadata>>(senders.size());
        kafkaProducer.beginTransaction();
        senders.forEach(sender -> futures.add(sender.run()));
        kafkaProducer.commitTransaction();
        return futures;
      }

      @Override
      public void flush() {
        kafkaProducer.flush();
      }

      @Override
      public void close() {
        kafkaProducer.close();
      }
    };
  }
}
