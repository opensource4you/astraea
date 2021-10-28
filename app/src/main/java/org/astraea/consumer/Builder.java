package org.astraea.consumer;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

public class Builder<Key, Value> {

  public enum OffsetPolicy {
    EARLIEST,
    LATEST
  }

  private final Map<String, Object> configs = new HashMap<>();
  private Deserializer<?> keyDeserializer = new ByteArrayDeserializer();
  private Deserializer<?> valueDeserializer = new ByteArrayDeserializer();
  private OffsetPolicy offsetPolicy = OffsetPolicy.LATEST;
  private String groupId = "groupId-" + System.currentTimeMillis();
  private final Set<String> topics = new HashSet<>();

  Builder() {}

  public Builder<Key, Value> groupId(String groupId) {
    this.groupId = Objects.requireNonNull(groupId);
    return this;
  }

  public Builder<Key, Value> offsetPolicy(OffsetPolicy offsetPolicy) {
    this.offsetPolicy = Objects.requireNonNull(offsetPolicy);
    return this;
  }

  public Builder<Key, Value> topics(Set<String> topics) {
    this.topics.addAll(topics);
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

  public Builder<Key, Value> configs(Map<String, Object> configs) {
    this.configs.putAll(configs);
    return this;
  }

  public Builder<Key, Value> brokers(String brokers) {
    this.configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(brokers));
    return this;
  }

  @SuppressWarnings("unchecked")
  public Consumer<Key, Value> build() {
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, Objects.requireNonNull(groupId));
    switch (offsetPolicy) {
      case EARLIEST:
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        break;
      case LATEST:
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        break;
    }
    var kafkaConsumer =
        new KafkaConsumer<>(
            configs, (Deserializer<Key>) keyDeserializer, (Deserializer<Value>) valueDeserializer);
    kafkaConsumer.subscribe(topics);
    return new Consumer<>() {
      @Override
      public Collection<Record<Key, Value>> poll(Duration timeout) {
        var records = kafkaConsumer.poll(timeout);
        return StreamSupport.stream(records.spliterator(), false)
            .map(
                r ->
                    new Record<Key, Value>() {

                      @Override
                      public String topic() {
                        return r.topic();
                      }

                      @Override
                      public int partition() {
                        return r.partition();
                      }

                      @Override
                      public Key key() {
                        return r.key();
                      }

                      @Override
                      public Value value() {
                        return r.value();
                      }

                      @Override
                      public long offset() {
                        return r.offset();
                      }

                      @Override
                      public long timestamp() {
                        return r.timestamp();
                      }

                      @Override
                      public int serializedKeySize() {
                        return r.serializedKeySize();
                      }

                      @Override
                      public int serializedValueSize() {
                        return r.serializedValueSize();
                      }
                    })
            .collect(Collectors.toList());
      }

      @Override
      public void wakeup() {
        kafkaConsumer.wakeup();
      }

      @Override
      public void close() {
        kafkaConsumer.close();
      }
    };
  }
}
