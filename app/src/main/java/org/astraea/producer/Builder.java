package org.astraea.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

public class Builder<Key, Value> {
  private final Map<String, Object> configs = new HashMap<>();
  private Serializer<?> keySerializer = new ByteArraySerializer();
  private Serializer<?> valueSerializer = new ByteArraySerializer();

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
            configs, (Serializer<Key>) keySerializer, (Serializer<Value>) valueSerializer);
    return new Producer<>() {

      @Override
      public Sender<Key, Value> sender() {
        return new Sender<>() {
          private Key key;
          private Value value;
          private String topic;
          private Integer partition;
          private Long timestamp;

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
          public CompletionStage<Metadata> run() {
            var completableFuture = new CompletableFuture<Metadata>();
            kafkaProducer.send(
                new ProducerRecord<>(topic, partition, timestamp, key, value),
                (metadata, exception) -> {
                  if (exception == null)
                    completableFuture.complete(
                        new Metadata() {

                          @Override
                          public long serializedKeySize() {
                            return metadata.serializedKeySize();
                          }

                          @Override
                          public long serializedValueSize() {
                            return metadata.serializedValueSize();
                          }

                          @Override
                          public long timestamp() {
                            return metadata.timestamp();
                          }

                          @Override
                          public String topic() {
                            return metadata.topic();
                          }

                          @Override
                          public int partition() {
                            return metadata.partition();
                          }
                        });
                  else completableFuture.completeExceptionally(exception);
                });
            return completableFuture;
          }
        };
      }

      @Override
      public void close() {
        kafkaProducer.close();
      }
    };
  }
}
