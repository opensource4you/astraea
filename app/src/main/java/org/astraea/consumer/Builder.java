package org.astraea.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Builder<Key, Value> {
  private final Map<String, Object> configs =
      new HashMap<>(
          Map.of(ConsumerConfig.GROUP_ID_CONFIG, "groupId-" + System.currentTimeMillis()));
  private Deserializer<?> keyDeserializer = Deserializer.BYTE_ARRAY;
  private Deserializer<?> valueDeserializer = Deserializer.BYTE_ARRAY;
  private final Set<String> topics = new HashSet<>();
  private ConsumerRebalanceListener listener = ignore -> {};

  Builder() {}

  public Builder<Key, Value> groupId(String groupId) {
    return config(ConsumerConfig.GROUP_ID_CONFIG, Objects.requireNonNull(groupId));
  }

  /**
   * make the consumer read data from beginning. By default, it reads the latest data.
   *
   * @return this builder
   */
  public Builder<Key, Value> fromBeginning() {
    return config(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
  }

  /**
   * make the consumer read data from latest. this is default setting.
   *
   * @return this builder
   */
  public Builder<Key, Value> fromLatest() {
    return config(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
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

  public Builder<Key, Value> config(String key, String value) {
    this.configs.put(key, value);
    return this;
  }

  public Builder<Key, Value> configs(Map<String, String> configs) {
    this.configs.putAll(configs);
    return this;
  }

  public Builder<Key, Value> brokers(String brokers) {
    return config(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(brokers));
  }

  public Builder<Key, Value> consumerRebalanceListener(ConsumerRebalanceListener listener) {
    this.listener = Objects.requireNonNull(listener);
    return this;
  }

  public Builder<Key, Value> isolation(Isolation isolation) {
    return config(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation.nameOfKafka());
  }

  @SuppressWarnings("unchecked")
  public Consumer<Key, Value> build() {
    var kafkaConsumer =
        new KafkaConsumer<>(
            configs,
            Deserializer.of((Deserializer<Key>) keyDeserializer),
            Deserializer.of((Deserializer<Value>) valueDeserializer));
    kafkaConsumer.subscribe(topics, ConsumerRebalanceListener.of(listener));
    return new Consumer<>() {
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
    };
  }
}
