package org.astraea.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Builder<Key, Value> {

  public enum OffsetPolicy {
    EARLIEST,
    LATEST
  }

  private final Map<String, Object> configs = new HashMap<>();
  private Deserializer<?> keyDeserializer = Deserializer.BYTE_ARRAY;
  private Deserializer<?> valueDeserializer = Deserializer.BYTE_ARRAY;
  private OffsetPolicy offsetPolicy = OffsetPolicy.LATEST;
  private String groupId = "groupId-" + System.currentTimeMillis();
  private final Set<String> topics = new HashSet<>();
  private ConsumerRebalanceListener listener = ignore -> {};

  Builder() {}

  public Builder<Key, Value> groupId(String groupId) {
    this.groupId = Objects.requireNonNull(groupId);
    return this;
  }

  /**
   * make the consumer read data from beginning. By default, it reads the latest data.
   *
   * @return this builder
   */
  public Builder<Key, Value> fromBeginning() {
    this.offsetPolicy = OffsetPolicy.EARLIEST;
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

  public Builder<Key, Value> consumerRebalanceListener(ConsumerRebalanceListener listener) {
    this.listener = Objects.requireNonNull(listener);
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
            configs,
            Deserializer.of((Deserializer<Key>) keyDeserializer),
            Deserializer.of((Deserializer<Value>) valueDeserializer));
    kafkaConsumer.subscribe(topics, ConsumerRebalanceListener.of(listener));
    return new Consumer<>() {
      @Override
      public Collection<Record<Key, Value>> poll(Duration timeout) {
        var records = kafkaConsumer.poll(timeout);
        return StreamSupport.stream(records.spliterator(), false)
            .map(Record::of)
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
