package org.astraea.workloads;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Deserializer;

public class ConsumerWorkloadBuilder<KeyType, ValueType> {

  private final Map<String, Object> configs;

  private ConsumerWorkloadBuilder() {
    this.configs = new HashMap<>();
  }

  public static ConsumerWorkloadBuilder<BytesSerializer, BytesSerializer> builder() {
    return new ConsumerWorkloadBuilder<>();
  }

  public ConsumerWorkloadBuilder<KeyType, ValueType> bootstrapServer(String bootstrapServer) {
    this.configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    return this;
  }

  public ConsumerWorkloadBuilder<KeyType, ValueType> consumerGroupId(String groupId) {
    this.configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    return this;
  }

  public ConsumerWorkloadBuilder<KeyType, ValueType> configs(Map<String, ?> configs) {
    this.configs.putAll(configs);
    return this;
  }

  @SuppressWarnings("unchecked")
  public <NewKeyType, KeyDeserializer extends Deserializer<NewKeyType>>
      ConsumerWorkloadBuilder<NewKeyType, ValueType> keyDeserializer(
          Class<KeyDeserializer> newKeyDeserializer) {
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, newKeyDeserializer);
    return (ConsumerWorkloadBuilder<NewKeyType, ValueType>) this;
  }

  @SuppressWarnings("unchecked")
  public <NewValueType, ValueDeserializer extends Deserializer<NewValueType>>
      ConsumerWorkloadBuilder<KeyType, NewValueType> valueDeserializer(
          Class<ValueDeserializer> newValueDeserializer) {
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, newValueDeserializer);
    return (ConsumerWorkloadBuilder<KeyType, NewValueType>) this;
  }

  public ConsumerWorkload<KeyType, ValueType> build(
      Consumer<org.apache.kafka.clients.consumer.Consumer<KeyType, ValueType>> workload) {
    return new ConsumerWorkload<>() {

      @Override
      public Map<String, Object> offerConfigs() {
        return configs;
      }

      @Override
      public void run(org.apache.kafka.clients.consumer.Consumer<KeyType, ValueType> consumer) {
        workload.accept(consumer);
      }
    };
  }
}
