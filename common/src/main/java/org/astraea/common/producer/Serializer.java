/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.producer;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;

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

  /**
   * create Custom JsonSerializer
   *
   * @return Custom JsonSerializer
   * @param <T> The type of message being output by the serializer
   */
  static <T> Serializer<T> of(TypeRef<T> typeRef) {
    return (topic, headers, data) -> new JsonSerializer<>(typeRef).serialize(topic, headers, data);
  }

  class JsonSerializer<T> implements Serializer<T> {
    private final String encoding = StandardCharsets.UTF_8.name();
    private final JsonConverter jackson = JsonConverter.jackson();

    private JsonSerializer(TypeRef<T> typeRef) {}

    @Override
    public byte[] serialize(String topic, Collection<Header> headers, T data) {
      if (data == null) {
        return null;
      }

      return Utils.packException(() -> jackson.toJson(data).getBytes(encoding));
    }
  }
}
