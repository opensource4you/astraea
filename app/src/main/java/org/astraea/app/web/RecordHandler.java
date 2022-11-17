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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.app.web.Request.RequestObject;
import org.astraea.common.Cache;
import org.astraea.common.EnumInfo;
import org.astraea.common.FutureUtils;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Partition;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.argument.DurationField;
import org.astraea.common.consumer.Builder;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.consumer.SeekStrategy;
import org.astraea.common.consumer.SubscribedConsumer;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.ProducerConfigs;
import org.astraea.common.producer.Serializer;

public class RecordHandler implements Handler {
  static final String RECORDS = "records";
  static final String TRANSACTION_ID = "transactionId";
  static final String PARTITION = "partition";
  static final String ASYNC = "async";
  static final String DISTANCE_FROM_LATEST = "distanceFromLatest";
  static final String DISTANCE_FROM_BEGINNING = "distanceFromBeginning";
  static final String SEEK_TO = "seekTo";
  static final String KEY_DESERIALIZER = "keyDeserializer";
  static final String VALUE_DESERIALIZER = "valueDeserializer";
  static final String LIMIT = "limit";
  static final String TIMEOUT = "timeout";
  static final String OFFSET = "offset";
  static final String GROUP_ID = "groupId";
  private static final int MAX_CACHE_SIZE = 100;
  private static final Duration CACHE_EXPIRE_DURATION = Duration.ofMinutes(10);

  final String bootstrapServers;
  // visible for testing
  final Producer<byte[], byte[]> producer;
  private final Cache<String, Producer<byte[], byte[]>> transactionalProducerCache;

  final Admin admin;

  RecordHandler(Admin admin, String bootstrapServers) {
    this.admin = admin;
    this.bootstrapServers = requireNonNull(bootstrapServers);
    this.producer = Producer.builder().bootstrapServers(bootstrapServers).build();
    this.transactionalProducerCache =
        Cache.<String, Producer<byte[], byte[]>>builder(
                transactionId ->
                    Producer.builder()
                        .config(ProducerConfigs.TRANSACTIONAL_ID_CONFIG, transactionId)
                        .bootstrapServers(bootstrapServers)
                        .buildTransactional())
            .maxCapacity(MAX_CACHE_SIZE)
            .expireAfterAccess(CACHE_EXPIRE_DURATION)
            .removalListener((k, v) -> v.close())
            .build();
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    if (channel.target().isEmpty()) throw new IllegalArgumentException("topic must be set");

    var topic = channel.target().get();
    var seekStrategies = Set.of(DISTANCE_FROM_LATEST, DISTANCE_FROM_BEGINNING, SEEK_TO);
    if (channel.queries().keySet().stream().filter(seekStrategies::contains).count() > 1)
      throw new IllegalArgumentException("only one seek strategy is allowed");

    var limit = Integer.parseInt(channel.queries().getOrDefault(LIMIT, "1"));
    var timeout =
        Optional.ofNullable(channel.queries().get(TIMEOUT))
            .map(DurationField::toDuration)
            .orElse(Duration.ofSeconds(5));

    var consumerBuilder =
        Optional.ofNullable(channel.queries().get(PARTITION))
            .map(Integer::valueOf)
            .map(
                partition ->
                    (Builder<byte[], byte[]>)
                        Consumer.forPartitions(Set.of(TopicPartition.of(topic, partition))))
            .orElseGet(
                () -> {
                  // disable auto commit here since we commit manually in Records#onComplete
                  var builder =
                      Consumer.forTopics(Set.of(topic))
                          .config(ConsumerConfigs.ENABLE_AUTO_COMMIT_CONFIG, "false");
                  Optional.ofNullable(channel.queries().get(GROUP_ID))
                      .ifPresent(
                          groupId -> builder.config(ConsumerConfigs.GROUP_ID_CONFIG, groupId));
                  return builder;
                });

    var keyDeserializer =
        Optional.ofNullable(channel.queries().get(KEY_DESERIALIZER))
            .map(SerDe::ofAlias)
            .orElse(SerDe.STRING)
            .deserializer;
    var valueDeserializer =
        Optional.ofNullable(channel.queries().get(VALUE_DESERIALIZER))
            .map(SerDe::ofAlias)
            .orElse(SerDe.STRING)
            .deserializer;
    consumerBuilder
        .bootstrapServers(bootstrapServers)
        .keyDeserializer(keyDeserializer)
        .valueDeserializer(valueDeserializer);

    Optional.ofNullable(channel.queries().get(DISTANCE_FROM_LATEST))
        .map(Long::parseLong)
        .ifPresent(
            distanceFromLatest ->
                consumerBuilder.seek(SeekStrategy.DISTANCE_FROM_LATEST, distanceFromLatest));

    Optional.ofNullable(channel.queries().get(DISTANCE_FROM_BEGINNING))
        .map(Long::parseLong)
        .ifPresent(
            distanceFromBeginning ->
                consumerBuilder.seek(SeekStrategy.DISTANCE_FROM_BEGINNING, distanceFromBeginning));

    Optional.ofNullable(channel.queries().get(SEEK_TO))
        .map(Long::parseLong)
        .ifPresent(seekTo -> consumerBuilder.seek(SeekStrategy.SEEK_TO, seekTo));

    return CompletableFuture.completedFuture(get(consumerBuilder.build(), limit, timeout));
  }

  // visible for testing
  GetResponse get(Consumer<byte[], byte[]> consumer, int limit, Duration timeout) {
    try {
      return new GetResponse(
          consumer, consumer.poll(limit, timeout).stream().map(Record::new).collect(toList()));
    } catch (Exception e) {
      consumer.close();
      throw e;
    }
  }

  static class RecordPostRequest implements Request {
    private boolean async = false;
    private String timeout = "5s";
    private List<PostRecord> records = List.of();

    private Optional<String> transactionId = Optional.empty();

    public RecordPostRequest() {}

    public boolean async() {
      return async;
    }

    public String timeout() {
      return timeout;
    }

    public List<PostRecord> records() {
      return records;
    }

    public Optional<String> transactionId() {
      return transactionId;
    }
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    var postRequest = channel.request().getRequest(TypeRef.of(RecordPostRequest.class));

    var records = postRequest.records();
    if (records.isEmpty())
      throw new IllegalArgumentException("records should contain at least one record");

    var producer =
        postRequest.transactionId().map(transactionalProducerCache::get).orElse(this.producer);

    var result =
        CompletableFuture.supplyAsync(
                () -> {
                  try {
                    return producer.send(
                        records.stream()
                            .map(record -> createRecord(producer, record))
                            .collect(toList()));
                  } finally {
                    if (producer.transactional()) {
                      producer.close();
                    }
                  }
                })
            .thenCompose(
                senderFutures ->
                    FutureUtils.sequence(
                        senderFutures.stream()
                            .map(
                                s ->
                                    s.handle(
                                        (metadata, exception) -> {
                                          if (metadata != null) return new Metadata(metadata);
                                          if (exception != null)
                                            return Response.for500(exception.getMessage());
                                          // this shouldn't happen, but still add error 404 here
                                          return Response.for404("missing result");
                                        }))
                            .map(CompletionStage::toCompletableFuture)
                            .collect(toList())));

    if (postRequest.async()) return CompletableFuture.completedFuture(Response.ACCEPT);
    return CompletableFuture.completedFuture(
        Utils.packException(
            () ->
                new PostResponse(
                    result.get(
                        DurationField.toDuration(postRequest.timeout()).toNanos(),
                        TimeUnit.NANOSECONDS))));
  }

  @Override
  public CompletionStage<Response> delete(Channel channel) {
    if (channel.target().isEmpty()) return CompletableFuture.completedFuture(Response.NOT_FOUND);
    var topic = channel.target().get();
    var partitions =
        Optional.ofNullable(channel.queries().get(PARTITION))
            .map(x -> CompletableFuture.completedStage(Set.of(TopicPartition.of(topic, x))))
            .orElseGet(() -> admin.topicPartitions(Set.of(topic)));

    return Optional.ofNullable(channel.queries().get(OFFSET))
        .map(Long::parseLong)
        .map(
            offset ->
                partitions.thenApply(
                    p -> p.stream().collect(Collectors.toMap(Function.identity(), x -> offset))))
        .orElseGet(
            () ->
                admin
                    .partitions(Set.of(topic))
                    .thenApply(
                        p ->
                            p.stream()
                                .collect(
                                    Collectors.toMap(
                                        Partition::topicPartition, Function.identity())))
                    .thenCompose(
                        topicPartitionPartitionMap ->
                            partitions.thenApply(
                                p ->
                                    p.stream()
                                        .collect(
                                            Collectors.toMap(
                                                Function.identity(),
                                                x ->
                                                    topicPartitionPartitionMap
                                                        .get(x)
                                                        .latestOffset())))))
        .thenCompose(admin::deleteRecords)
        .thenApply(records -> Response.OK);
  }

  enum SerDe implements EnumInfo {
    BYTEARRAY(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(x -> JsonConverter.defaultConverter().fromJson(x, TypeRef.bytes()))
                .orElse(null),
        Deserializer.BYTE_ARRAY),
    STRING(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(x -> JsonConverter.defaultConverter().fromJson(x, TypeRef.of(String.class)))
                .map(v -> Serializer.STRING.serialize(topic, List.of(), v))
                .orElse(null),
        Deserializer.STRING),
    LONG(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(x -> JsonConverter.defaultConverter().fromJson(x, TypeRef.of(Long.class)))
                .map(longVal -> Serializer.LONG.serialize(topic, List.of(), longVal))
                .orElse(null),
        Deserializer.LONG),
    INTEGER(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(x -> JsonConverter.defaultConverter().fromJson(x, TypeRef.of(Integer.class)))
                .map(intVal -> Serializer.INTEGER.serialize(topic, List.of(), intVal))
                .orElse(null),
        Deserializer.INTEGER),
    FLOAT(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(x -> JsonConverter.defaultConverter().fromJson(x, TypeRef.of(Float.class)))
                .map(floatVal -> Serializer.FLOAT.serialize(topic, List.of(), floatVal))
                .orElse(null),
        Deserializer.FLOAT),
    DOUBLE(
        (topic, value) ->
            Optional.ofNullable(value)
                .map(x -> JsonConverter.defaultConverter().fromJson(x, TypeRef.of(Double.class)))
                .map(doubleVal -> Serializer.DOUBLE.serialize(topic, List.of(), doubleVal))
                .orElse(null),
        Deserializer.DOUBLE);

    static SerDe ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(SerDe.class, alias);
    }

    @Override
    public String alias() {
      return name();
    }

    @Override
    public String toString() {
      return alias();
    }

    /** (topic, json) convert to bytes */
    final BiFunction<String, String, byte[]> serializer;

    final Deserializer<?> deserializer;

    SerDe(BiFunction<String, String, byte[]> serializer, Deserializer<?> deserializer) {
      this.serializer = requireNonNull(serializer);
      this.deserializer = requireNonNull(deserializer);
    }
  }

  private static org.astraea.common.producer.Record<byte[], byte[]> createRecord(
      Producer<byte[], byte[]> producer, PostRecord postRecord) {
    var topic = postRecord.topic;
    var builder = org.astraea.common.producer.Record.<byte[], byte[]>builder().topic(topic);

    // TODO: Support headers
    // (https://github.com/skiptests/astraea/issues/422)
    var keySerializer = SerDe.ofAlias(postRecord.keySerializer).serializer;
    var valueSerializer = SerDe.ofAlias(postRecord.valueSerializer).serializer;
    postRecord.key.ifPresent(
        key ->
            builder.key(keySerializer.apply(topic, JsonConverter.defaultConverter().toJson(key))));
    postRecord.value.ifPresent(
        value ->
            builder.value(
                valueSerializer.apply(topic, JsonConverter.defaultConverter().toJson(value))));
    postRecord.timestamp.ifPresent(builder::timestamp);
    postRecord.partition.ifPresent(builder::partition);
    return builder.build();
  }

  static class Metadata implements Response {
    final String topic;
    final int partition;
    final long offset;
    final long timestamp;
    final int serializedKeySize;
    final int serializedValueSize;

    Metadata(org.astraea.common.producer.Metadata metadata) {
      topic = metadata.topic();
      partition = metadata.partition();
      offset = metadata.offset();
      timestamp = metadata.timestamp();
      serializedKeySize = metadata.serializedKeySize();
      serializedValueSize = metadata.serializedValueSize();
    }
  }

  static class GetResponse implements Response {
    final transient Consumer<byte[], byte[]> consumer;
    final Collection<Record> records;

    private GetResponse(Consumer<byte[], byte[]> consumer, Collection<Record> records) {
      this.consumer = consumer;
      this.records = records;
    }

    @Override
    public String json() {
      return JsonConverter.defaultConverter().toJson(this);
    }

    @Override
    public void onComplete(Throwable error) {
      try {
        if (error == null && consumer instanceof SubscribedConsumer) {
          ((SubscribedConsumer<byte[], byte[]>) consumer).commitOffsets(Duration.ofSeconds(5));
        }
      } finally {
        consumer.close();
      }
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

    Record(org.astraea.common.consumer.Record<?, ?> record) {
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

    Header(org.astraea.common.Header header) {
      this.key = header.key();
      this.value = header.value();
    }
  }

  static class PostRecord implements RequestObject {
    String topic;
    Optional<Integer> partition = Optional.empty();
    String keySerializer = "STRING";
    String valueSerializer = "STRING";
    Optional<Object> key = Optional.empty();
    Optional<Object> value = Optional.empty();
    Optional<Long> timestamp = Optional.empty();

    public PostRecord() {}

    PostRecord(
        String topic,
        Integer partition,
        String keySerializer,
        String valueSerializer,
        Object key,
        Object value,
        Long timestamp) {
      this.topic = topic;
      this.partition = Optional.ofNullable(partition);
      this.keySerializer = Optional.ofNullable(keySerializer).orElse("STRING");
      this.valueSerializer = Optional.ofNullable(valueSerializer).orElse("STRING");
      this.key = Optional.ofNullable(key);
      this.value = Optional.ofNullable(value);
      this.timestamp = Optional.ofNullable(timestamp);
    }
  }

  static class PostResponse implements Response {
    final List<Response> results;

    PostResponse(List<Response> results) {
      this.results = results;
    }
  }
}
