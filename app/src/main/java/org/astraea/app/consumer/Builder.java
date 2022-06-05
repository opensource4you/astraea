package org.astraea.app.consumer;

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
  private int distanceFromLatest = -1;

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

  public Builder<Key, Value> bootstrapServers(String bootstrapServers) {
    return config(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers));
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

    // this mode is not supported by kafka, so we have to calculate the offset first
    if (distanceFromLatest > 0) {
      // 1) poll data until the assignment is completed
      while (kafkaConsumer.assignment().isEmpty()) kafkaConsumer.poll(Duration.ofMillis(500));
      var partitions = kafkaConsumer.assignment();
      // 2) get the end offsets from all subscribed partitions
      var endOffsets = kafkaConsumer.endOffsets(partitions);
      // 3) calculate and then seek to the correct offset (end offset - recent offset)
      endOffsets.forEach(
          (tp, latest) -> kafkaConsumer.seek(tp, Math.max(0, latest - distanceFromLatest)));
    }
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
