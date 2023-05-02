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
import org.astraea.common.ByteUtils;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.BeanObject;

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

  Serializer<byte[]> BYTE_ARRAY = (topic, headers, data) -> data;
  Serializer<String> STRING =
      (topic, headers, data) -> data == null ? null : ByteUtils.toBytes(data);
  Serializer<Integer> INTEGER =
      (topic, headers, data) -> data == null ? null : ByteUtils.toBytes(data);
  Serializer<Long> LONG = (topic, headers, data) -> data == null ? null : ByteUtils.toBytes(data);
  Serializer<Float> FLOAT = (topic, headers, data) -> data == null ? null : ByteUtils.toBytes(data);
  Serializer<Double> DOUBLE =
      (topic, headers, data) -> data == null ? null : ByteUtils.toBytes(data);
  Serializer<BeanObject> BEAN_OBJECT = (topic, headers, data) -> ByteUtils.toBytes(data);

  /**
   * create Custom JsonSerializer
   *
   * @return Custom JsonSerializer
   * @param <T> The type of message being output by the serializer
   */
  static <T> Serializer<T> of(TypeRef<T> typeRef) {
    return new JsonSerializer<>(typeRef);
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
