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
import static java.util.stream.Collectors.toList;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.argument.DurationField;
import org.astraea.app.common.Utils;
import org.astraea.app.consumer.Builder;
import org.astraea.app.consumer.Consumer;
import org.astraea.app.consumer.Deserializer;
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
  static final String DISTANCE_FROM_LATEST = "distanceFromLatest";
  static final String DISTANCE_FROM_BEGINNING = "distanceFromBeginning";
  static final String SEEK_TO = "seekTo";
  static final String KEY_DESERIALIZER = "keyDeserializer";
  static final String VALUE_DESERIALIZER = "valueDeserializer";
  static final String RECORDS = "records";
  static final String TIMEOUT = "timeout";

  final String bootstrapServers;
  // visible for testing
  final Producer<byte[], byte[]> producer;

  RecordHandler(String bootstrapServers) {
    this.bootstrapServers = requireNonNull(bootstrapServers);
    this.producer = Producer.builder().bootstrapServers(bootstrapServers).build();
  }

  @Override
  public Response get(Optional<String> target, Map<String, String> queries) {
    var topic = target.orElseThrow(() -> new IllegalArgumentException("topic must be set"));
    var seekStrategies = Set.of(DISTANCE_FROM_LATEST, DISTANCE_FROM_BEGINNING, SEEK_TO);
    if (queries.keySet().stream().filter(seekStrategies::contains).count() > 1) {
      throw new IllegalArgumentException("only one seek strategy is allowed");
    }

    var timeout =
        Optional.ofNullable(queries.get(TIMEOUT))
            .map(DurationField::toDuration)
            .orElse(Duration.ofSeconds(5));

    var consumerBuilder =
        Optional.ofNullable(queries.get(PARTITION))
            .map(Integer::valueOf)
            .map(
                partition ->
                    (Builder<byte[], byte[]>)
                        Consumer.forPartitions(Set.of(new TopicPartition(topic, partition))))
            .orElseGet(() -> Consumer.forTopics(Set.of(topic)));

    var keyDeserializer =
        Optional.ofNullable(queries.get(KEY_DESERIALIZER))
            .map(SerDe::of)
            .orElse(SerDe.STRING)
            .deserializer;
    var valueDeserializer =
        Optional.ofNullable(queries.get(VALUE_DESERIALIZER))
            .map(SerDe::of)
            .orElse(SerDe.STRING)
            .deserializer;
    consumerBuilder
        .bootstrapServers(bootstrapServers)
        .keyDeserializer(keyDeserializer)
        .valueDeserializer(valueDeserializer);

    Optional.ofNullable(queries.get(DISTANCE_FROM_LATEST))
        .map(Long::parseLong)
        .ifPresent(
            distanceFromLatest ->
                consumerBuilder.seekStrategy(
                    Builder.SeekStrategy.DISTANCE_FROM_LATEST, distanceFromLatest));

    Optional.ofNullable(queries.get(DISTANCE_FROM_BEGINNING))
        .map(Long::parseLong)
        .ifPresent(
            distanceFromBeginning ->
                consumerBuilder.seekStrategy(
                    Builder.SeekStrategy.DISTANCE_FROM_BEGINNING, distanceFromBeginning));

    Optional.ofNullable(queries.get(SEEK_TO))
        .map(Long::parseLong)
        .ifPresent(seekTo -> consumerBuilder.seekStrategy(Builder.SeekStrategy.SEEK_TO, seekTo));

    try (var consumer = consumerBuilder.build()) {
      var maxRecords = Integer.parseInt(queries.getOrDefault(RECORDS, "1"));
      return new Records(
          consumer.poll(maxRecords, timeout).stream()
              .map(Record::new)
              // TODO: remove limit here (https://github.com/skiptests/astraea/issues/441)
              .limit(maxRecords)
              .collect(toList()));
    }
  }

  @Override
  public Response post(PostRequest request) {
    var topic =
        request.get(TOPIC).orElseThrow(() -> new IllegalArgumentException("topic must be set"));
    var keySerializer = request.get(KEY_SERIALIZER).map(SerDe::of).orElse(SerDe.STRING).serializer;
    var valueSerializer =
        request.get(VALUE_SERIALIZER).map(SerDe::of).orElse(SerDe.STRING).serializer;
    var async = request.booleanValue(ASYNC, false);
    var sender = producer.sender().topic(topic);

    // TODO: Support headers (https://github.com/skiptests/astraea/issues/422)
    request.get(KEY).ifPresent(k -> sender.key(keySerializer.apply(topic, k)));
    request.get(VALUE).ifPresent(v -> sender.value(valueSerializer.apply(topic, v)));
    request.get(TIMESTAMP).ifPresent(t -> sender.timestamp(Long.parseLong(t)));
    request.get(PARTITION).ifPresent(p -> sender.partition(Integer.parseInt(p)));

    var senderFuture = sender.run().toCompletableFuture();

    if (async) return Response.ACCEPT;

    return Utils.packException(() -> new Metadata(senderFuture.get()));
  }

  enum SerDe {
    BYTEARRAY(
        (topic, value) ->
            Optional.ofNullable(value).map(v -> Base64.getDecoder().decode(v)).orElse(null),
        Deserializer.BYTE_ARRAY),
    STRING(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(v -> Serializer.STRING.serialize(topic, List.of(), value))
                .orElse(null),
        Deserializer.STRING),
    LONG(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(Long::parseLong)
                .map(longVal -> Serializer.LONG.serialize(topic, List.of(), longVal))
                .orElse(null),
        Deserializer.LONG),
    INTEGER(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(Integer::parseInt)
                .map(intVal -> Serializer.INTEGER.serialize(topic, List.of(), intVal))
                .orElse(null),
        Deserializer.INTEGER),
    FLOAT(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(Float::parseFloat)
                .map(floatVal -> Serializer.FLOAT.serialize(topic, List.of(), floatVal))
                .orElse(null),
        Deserializer.FLOAT),
    DOUBLE(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(Double::parseDouble)
                .map(doubleVal -> Serializer.DOUBLE.serialize(topic, List.of(), doubleVal))
                .orElse(null),
        Deserializer.DOUBLE);

    final BiFunction<String, String, byte[]> serializer;
    final Deserializer<?> deserializer;

    SerDe(BiFunction<String, String, byte[]> serializer, Deserializer<?> deserializer) {
      this.serializer = requireNonNull(serializer);
      this.deserializer = requireNonNull(deserializer);
    }

    static SerDe of(String name) {
      return valueOf(name.toUpperCase(Locale.ROOT));
    }
  }

  static class Metadata implements Response {
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

  static class Records implements Response {
    final Collection<Record> data;

    Records(Collection<Record> data) {
      this.data = data;
    }

    @Override
    public String json() {
      return new GsonBuilder()
          // gson will do html escape by default (e.g. convert = to \u003d)
          .disableHtmlEscaping()
          .registerTypeHierarchyAdapter(byte[].class, new ByteArrayToBase64TypeAdapter())
          .create()
          .toJson(this);
    }
  }

  static class Record {
    final String topic;
    final int partition;
    final long offset;
    final long timestamp;
    final int serializedKeySize;
    final int serializedValueSize;
    final Collection<Header> headers;
    final Object key;
    final Object value;
    final Integer leaderEpoch;

    Record(org.astraea.app.consumer.Record<?, ?> record) {
      topic = record.topic();
      partition = record.partition();
      offset = record.offset();
      timestamp = record.timestamp();
      serializedKeySize = record.serializedKeySize();
      serializedValueSize = record.serializedValueSize();
      headers = record.headers().stream().map(Header::new).collect(toList());
      key = record.key();
      value = record.value();
      leaderEpoch = record.leaderEpoch().orElse(null);
    }
  }

  static class Header {
    final String key;
    final byte[] value;

    Header(org.astraea.app.consumer.Header header) {
      this.key = header.key();
      this.value = header.value();
    }
  }

  static class ByteArrayToBase64TypeAdapter
      implements JsonSerializer<byte[]>, JsonDeserializer<byte[]> {
    public byte[] deserialize(JsonElement json, Type type, JsonDeserializationContext context)
        throws JsonParseException {
      return Base64.getDecoder().decode(json.getAsString());
    }

    public JsonElement serialize(byte[] src, Type type, JsonSerializationContext context) {
      return new JsonPrimitive(Base64.getEncoder().encodeToString(src));
    }
  }
}
