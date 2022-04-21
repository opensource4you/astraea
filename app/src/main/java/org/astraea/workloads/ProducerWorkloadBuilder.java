package org.astraea.workloads;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;

public class ProducerWorkloadBuilder<KeyType, ValueType> {

  private final Map<String, Object> configs;

  private ProducerWorkloadBuilder() {
    this.configs = new HashMap<>();
  }

  public static ProducerWorkloadBuilder<Bytes, Bytes> builder() {
    return new ProducerWorkloadBuilder<>();
  }

  public ProducerWorkloadBuilder<KeyType, ValueType> bootstrapServer(String bootstrapServer) {
    this.configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    return this;
  }

  public ProducerWorkloadBuilder<KeyType, ValueType> configs(Map<String, ?> configs) {
    this.configs.putAll(configs);
    return this;
  }

  @SuppressWarnings("unchecked")
  public <NewKeyType, KeySerializer extends Serializer<NewKeyType>>
      ProducerWorkloadBuilder<NewKeyType, ValueType> keySerializer(
          Class<KeySerializer> newKeySerializer) {
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, newKeySerializer);
    return (ProducerWorkloadBuilder<NewKeyType, ValueType>) this;
  }

  @SuppressWarnings("unchecked")
  public <NewValueType, KeySerializer extends Serializer<NewValueType>>
      ProducerWorkloadBuilder<KeyType, NewValueType> valueSerializer(
          Class<KeySerializer> newValueSerializer) {
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, newValueSerializer);
    return (ProducerWorkloadBuilder<KeyType, NewValueType>) this;
  }

  public ProducerWorkload<KeyType, ValueType> build(
      Consumer<Producer<KeyType, ValueType>> workload) {
    return new ProducerWorkload<>() {

      @Override
      public Map<String, Object> offerConfigs() {
        return configs;
      }

      @Override
      public void run(Producer<KeyType, ValueType> producer) {
        workload.accept(producer);
      }
    };
  }
}
