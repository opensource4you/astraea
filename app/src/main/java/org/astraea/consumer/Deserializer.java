package org.astraea.consumer;

import java.util.Collection;
import java.util.List;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.*;

@FunctionalInterface
public interface Deserializer<T> {

  /**
   * Deserialize a record value from a byte array into a value or object.
   *
   * @param topic topic associated with the data
   * @param headers headers associated with the record; may be empty.
   * @param data serialized bytes; may be null; implementations are recommended to handle null by
   *     returning a value or null rather than throwing an exception.
   * @return deserialized typed data; may be null
   */
  T deserialize(String topic, Collection<Header> headers, byte[] data);

  static <T> org.apache.kafka.common.serialization.Deserializer<T> of(
      Deserializer<T> deserializer) {
    return new org.apache.kafka.common.serialization.Deserializer<>() {

      @Override
      public T deserialize(String topic, byte[] data) {
        return deserializer.deserialize(topic, List.of(), data);
      }

      @Override
      public T deserialize(String topic, Headers headers, byte[] data) {
        return deserializer.deserialize(topic, Header.of(headers), data);
      }
    };
  }

  static <T> Deserializer<T> of(
      org.apache.kafka.common.serialization.Deserializer<T> deserializer) {
    return (topic, headers, data) -> deserializer.deserialize(topic, Header.of(headers), data);
  }

  Deserializer<byte[]> BYTE_ARRAY = of(new ByteArrayDeserializer());
  Deserializer<String> STRING = of(new StringDeserializer());
  Deserializer<Integer> INTEGER = of(new IntegerDeserializer());
  Deserializer<Long> LONG = of(new LongDeserializer());
  Deserializer<Float> FLOAT = of(new FloatDeserializer());
  Deserializer<Double> DOUBLE = of(new DoubleDeserializer());
}
