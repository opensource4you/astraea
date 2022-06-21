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
package org.astraea.app.web;

import static java.util.Objects.requireNonNull;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.astraea.app.common.Utils;
import org.astraea.app.producer.Producer;
import org.astraea.app.producer.Serializer;

public class RecordHandler implements Handler {
  static final String KEY_SERIALIZER = "keySerializer";
  static final String VALUE_SERIALIZER = "valueSerializer";
  static final String TOPIC = "topic";
  static final String KEY = "key";
  static final String VALUE = "value";
  static final String TIMESTAMP = "timestamp";
  static final String PARTITION = "partition";
  static final String ASYNC = "async";

  // visible for testing
  final Producer<byte[], byte[]> producer;

  RecordHandler(String bootstrapServers) {
    this.producer = Producer.builder().bootstrapServers(requireNonNull(bootstrapServers)).build();
  }

  @Override
  public JsonObject get(Optional<String> target, Map<String, String> queries) {
    // TODO: Implement topic read web apis (https://github.com/skiptests/astraea/pull/405)
    return ErrorObject.for404("GET is not supported yet");
  }

  @Override
  public JsonObject post(PostRequest request) {
    var topic =
        request.get(TOPIC).orElseThrow(() -> new IllegalArgumentException("topic must be set"));
    var keySerDe =
        SerDe.valueOf(
            request.get(KEY_SERIALIZER).map(String::toUpperCase).orElse(SerDe.STRING.name()));
    var valueSerDe =
        SerDe.valueOf(
            request.get(VALUE_SERIALIZER).map(String::toUpperCase).orElse(SerDe.STRING.name()));
    var async = request.booleanValue(ASYNC, false);
    var sender = producer.sender().topic(topic);

    // TODO: Support headers (https://github.com/skiptests/astraea/issues/422)
    request.get(KEY).ifPresent(k -> sender.key(keySerDe.serializer.apply(topic, k)));
    request.get(VALUE).ifPresent(v -> sender.value(valueSerDe.serializer.apply(topic, v)));
    request.get(TIMESTAMP).ifPresent(t -> sender.timestamp(Long.parseLong(t)));
    request.get(PARTITION).ifPresent(p -> sender.partition(Integer.parseInt(p)));

    var senderFuture = sender.run().toCompletableFuture();

    if (async) {
      // TODO: Return HTTP status code 202 instead of 200
      //  (https://github.com/skiptests/astraea/issues/420)
      return new JsonObject() {};
    }
    return Utils.packException(() -> new Metadata(senderFuture.get()));
  }

  enum SerDe {
    BYTEARRAY(
        (topic, value) ->
            Optional.ofNullable(value).map(v -> Base64.getDecoder().decode(v)).orElse(null)),
    STRING(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(v -> Serializer.STRING.serialize(topic, List.of(), value))
                .orElse(null)),
    LONG(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(Long::parseLong)
                .map(longVal -> Serializer.LONG.serialize(topic, List.of(), longVal))
                .orElse(null)),
    INTEGER(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(Integer::parseInt)
                .map(intVal -> Serializer.INTEGER.serialize(topic, List.of(), intVal))
                .orElse(null)),
    FLOAT(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(Float::parseFloat)
                .map(floatVal -> Serializer.FLOAT.serialize(topic, List.of(), floatVal))
                .orElse(null)),
    DOUBLE(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(Double::parseDouble)
                .map(doubleVal -> Serializer.DOUBLE.serialize(topic, List.of(), doubleVal))
                .orElse(null));

    final BiFunction<String, String, byte[]> serializer;

    SerDe(BiFunction<String, String, byte[]> serializer) {
      this.serializer = requireNonNull(serializer);
    }
  }

  static class Metadata implements JsonObject {
    final String topic;
    final int partition;
    final long offset;
    final long timestamp;
    final int serializedKeySize;
    final int serializedValueSize;

    Metadata(org.astraea.app.producer.Metadata metadata) {
      topic = metadata.topic();
      partition = metadata.partition();
      offset = metadata.offset();
      timestamp = metadata.timestamp();
      serializedKeySize = metadata.serializedKeySize();
      serializedValueSize = metadata.serializedValueSize();
    }
  }
}
