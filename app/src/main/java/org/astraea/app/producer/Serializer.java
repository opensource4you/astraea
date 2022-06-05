package org.astraea.app.producer;

import java.util.Collection;
import java.util.List;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.astraea.app.consumer.Header;

@FunctionalInterface
public interface Serializer<T> {

  /**
   * Convert {@code data} into a byte array.
   *
   * @param topic topic associated with data
   * @param headers headers associated with the record
   * @param data typed data
   * @return serialized bytes
   */
  byte[] serialize(String topic, Collection<Header> headers, T data);

  static <T> org.apache.kafka.common.serialization.Serializer<T> of(Serializer<T> serializer) {
    return new org.apache.kafka.common.serialization.Serializer<>() {

      @Override
      public byte[] serialize(String topic, T data) {
        return serializer.serialize(topic, List.of(), data);
      }

      @Override
      public byte[] serialize(String topic, Headers headers, T data) {
        return serializer.serialize(topic, Header.of(headers), data);
      }
    };
  }

  static <T> Serializer<T> of(org.apache.kafka.common.serialization.Serializer<T> serializer) {
    return (topic, headers, data) -> serializer.serialize(topic, Header.of(headers), data);
  }

  Serializer<byte[]> BYTE_ARRAY = of(new ByteArraySerializer());
  Serializer<String> STRING = of(new StringSerializer());
  Serializer<Integer> INTEGER = of(new IntegerSerializer());
  Serializer<Long> LONG = of(new LongSerializer());
  Serializer<Float> FLOAT = of(new FloatSerializer());
  Serializer<Double> DOUBLE = of(new DoubleSerializer());
}
