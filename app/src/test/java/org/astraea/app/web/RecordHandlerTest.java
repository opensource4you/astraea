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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.astraea.app.web.RecordHandler.ASYNC;
import static org.astraea.app.web.RecordHandler.DISTANCE_FROM_BEGINNING;
import static org.astraea.app.web.RecordHandler.DISTANCE_FROM_LATEST;
import static org.astraea.app.web.RecordHandler.GROUP_ID;
import static org.astraea.app.web.RecordHandler.KEY_DESERIALIZER;
import static org.astraea.app.web.RecordHandler.LIMIT;
import static org.astraea.app.web.RecordHandler.OFFSET;
import static org.astraea.app.web.RecordHandler.PARTITION;
import static org.astraea.app.web.RecordHandler.RECORDS;
import static org.astraea.app.web.RecordHandler.SEEK_TO;
import static org.astraea.app.web.RecordHandler.TIMEOUT;
import static org.astraea.app.web.RecordHandler.TRANSACTION_ID;
import static org.astraea.app.web.RecordHandler.VALUE_DESERIALIZER;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.app.web.RecordHandler.ByteArrayToBase64TypeAdapter;
import org.astraea.app.web.RecordHandler.Metadata;
import org.astraea.common.ExecutionRuntimeException;
import org.astraea.common.Utils;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.consumer.Header;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class RecordHandlerTest extends RequireBrokerCluster {

  @Test
  void testInvalidPost() {
    var handler = getRecordHandler();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            handler
                .post(Channel.ofRequest(PostRequest.of(Map.of(RECORDS, "[]"))))
                .toCompletableFuture()
                .get(),
        "records should contain at least one record");
    var executionRuntimeException =
        Assertions.assertThrows(
            ExecutionRuntimeException.class,
            () -> handler.post(Channel.ofRequest(PostRequest.of(Map.of(RECORDS, "[{}]")))),
            "topic must be set");
    Assertions.assertEquals(
        IllegalArgumentException.class, executionRuntimeException.getRootCause().getClass());
  }

  @Test
  void testPostTimeout() throws ExecutionException, InterruptedException {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> getRecordHandler().post(Channel.ofRequest(PostRequest.of(Map.of(TIMEOUT, "foo")))));
    Assertions.assertInstanceOf(
        RecordHandler.PostResponse.class,
        getRecordHandler()
            .post(
                Channel.ofRequest(
                    PostRequest.of(
                        new Gson()
                            .toJson(
                                Map.of(
                                    TIMEOUT,
                                    "10s",
                                    RECORDS,
                                    List.of(
                                        new RecordHandler.PostRecord(
                                            "test", null, null, null, null, null, null)))))))
            .toCompletableFuture()
            .get());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testPost(boolean isTransaction) throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    var currentTimestamp = System.currentTimeMillis();
    var requestParams = new HashMap<String, Object>();
    requestParams.put(
        RECORDS,
        List.of(
            new RecordHandler.PostRecord(
                topic, 0, "string", "integer", "foo", 100, currentTimestamp),
            new RecordHandler.PostRecord(
                topic, 0, "string", "integer", "bar", 200, currentTimestamp)));
    if (isTransaction) {
      requestParams.put(TRANSACTION_ID, "trx-" + topic);
    }
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.PostResponse.class,
            getRecordHandler()
                .post(Channel.ofRequest(PostRequest.of(new Gson().toJson(requestParams))))
                .toCompletableFuture()
                .get());

    Assertions.assertEquals(2, response.results.size());

    var results = response.results;
    var metadata = (Metadata) results.get(0);
    Assertions.assertEquals(0, metadata.offset);
    Assertions.assertEquals(0, metadata.partition);
    Assertions.assertEquals(topic, metadata.topic);
    Assertions.assertEquals("foo".getBytes(UTF_8).length, metadata.serializedKeySize);
    Assertions.assertEquals(4, metadata.serializedValueSize);

    metadata = (Metadata) results.get(1);
    Assertions.assertEquals(1, metadata.offset);
    Assertions.assertEquals(0, metadata.partition);
    Assertions.assertEquals(topic, metadata.topic);
    Assertions.assertEquals("bar".getBytes(UTF_8).length, metadata.serializedKeySize);
    Assertions.assertEquals(4, metadata.serializedValueSize);

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .keyDeserializer(Deserializer.STRING)
            .valueDeserializer(Deserializer.INTEGER)
            .build()) {
      var records = List.copyOf(consumer.poll(2, Duration.ofSeconds(10)));
      Assertions.assertEquals(2, records.size());

      var record = records.get(0);
      Assertions.assertEquals(topic, record.topic());
      Assertions.assertEquals(currentTimestamp, record.timestamp());
      Assertions.assertEquals(0, record.partition());
      Assertions.assertEquals(0, record.offset());
      Assertions.assertEquals("foo".getBytes(UTF_8).length, record.serializedKeySize());
      Assertions.assertEquals(4, record.serializedValueSize());
      Assertions.assertEquals("foo", record.key());
      Assertions.assertEquals(100, record.value());

      record = records.get(1);
      Assertions.assertEquals(topic, record.topic());
      Assertions.assertEquals(currentTimestamp, record.timestamp());
      Assertions.assertEquals(0, record.partition());
      Assertions.assertEquals(1, record.offset());
      Assertions.assertEquals("bar".getBytes(UTF_8).length, record.serializedKeySize());
      Assertions.assertEquals(4, record.serializedValueSize());
      Assertions.assertEquals("bar", record.key());
      Assertions.assertEquals(200, record.value());
    }
  }

  @Test
  void testPostWithAsync() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    var handler = getRecordHandler();
    var currentTimestamp = System.currentTimeMillis();
    var result =
        Assertions.assertInstanceOf(
            Response.class,
            handler
                .post(
                    Channel.ofRequest(
                        PostRequest.of(
                            new Gson()
                                .toJson(
                                    Map.of(
                                        ASYNC,
                                        "true",
                                        RECORDS,
                                        List.of(
                                            new RecordHandler.PostRecord(
                                                topic,
                                                0,
                                                "string",
                                                "integer",
                                                "foo",
                                                "100",
                                                currentTimestamp)))))))
                .toCompletableFuture()
                .get());
    Assertions.assertEquals(Response.ACCEPT, result);

    handler.producer.flush();

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .build()) {
      var record = consumer.poll(1, Duration.ofSeconds(10)).iterator().next();
      Assertions.assertEquals(topic, record.topic());
      Assertions.assertEquals(currentTimestamp, record.timestamp());
      Assertions.assertEquals(0, record.partition());
      Assertions.assertEquals("foo".getBytes(UTF_8).length, record.serializedKeySize());
      Assertions.assertEquals(4, record.serializedValueSize());
      Assertions.assertArrayEquals("foo".getBytes(UTF_8), record.key());
      Assertions.assertArrayEquals(ByteBuffer.allocate(4).putInt(100).array(), record.value());
    }
  }

  @ParameterizedTest
  @MethodSource("forTestSerializer")
  void testSerializer(String serializer, String actual, byte[] expected)
      throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    var handler = getRecordHandler();
    Assertions.assertInstanceOf(
        RecordHandler.PostResponse.class,
        handler
            .post(
                Channel.ofRequest(
                    PostRequest.of(
                        new Gson()
                            .toJson(
                                Map.of(
                                    RECORDS,
                                    List.of(
                                        new RecordHandler.PostRecord(
                                            topic, null, serializer, null, actual, null, null)))))))
            .toCompletableFuture()
            .get());

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .build()) {
      var record = consumer.poll(1, Duration.ofSeconds(10)).iterator().next();
      Assertions.assertArrayEquals(expected, record.key());
    }
  }

  private static Stream<Arguments> forTestSerializer() {
    return Stream.of(
        arguments("integer", "10", ByteBuffer.allocate(Integer.BYTES).putInt(10).array()),
        arguments("long", "11", ByteBuffer.allocate(Long.BYTES).putLong(11).array()),
        arguments("float", "0.1", ByteBuffer.allocate(Float.BYTES).putFloat(0.1f).array()),
        arguments("double", "0.1", ByteBuffer.allocate(Double.BYTES).putDouble(0.1).array()),
        arguments("string", "astraea", "astraea".getBytes(UTF_8)),
        arguments(
            "bytearray",
            Base64.getEncoder().encodeToString("astraea".getBytes(UTF_8)),
            "astraea".getBytes(UTF_8)));
  }

  private static void produceData(String topic, int size) {
    try (var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      IntStream.range(0, size)
          .forEach(
              i ->
                  producer
                      .sender()
                      .topic(topic)
                      .value(ByteBuffer.allocate(Integer.BYTES).putInt(i).array())
                      .run());
      producer.flush();
    }
  }

  @Test
  void testInvalidGet() throws ExecutionException, InterruptedException {
    var handler = getRecordHandler();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> handler.get(Channel.EMPTY).toCompletableFuture().get().code());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            handler
                .get(
                    Channel.ofQueries(
                        "topic", Map.of(DISTANCE_FROM_BEGINNING, "1", DISTANCE_FROM_LATEST, "1")))
                .toCompletableFuture()
                .get(),
        "only one seek strategy is allowed");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            handler
                .get(
                    Channel.ofQueries(
                        "topic",
                        Map.of(
                            DISTANCE_FROM_BEGINNING, "1", DISTANCE_FROM_LATEST, "1", SEEK_TO, "1")))
                .toCompletableFuture()
                .get(),
        "only one seek strategy is allowed");
  }

  @Test
  void testDistanceFromLatest() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    produceData(topic, 10);

    var handler = getRecordHandler();
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.GetResponse.class,
            handler
                .get(
                    Channel.ofQueries(
                        topic, Map.of(DISTANCE_FROM_LATEST, "2", VALUE_DESERIALIZER, "integer")))
                .toCompletableFuture()
                .get());

    Assertions.assertEquals(2, response.records.size());
    Assertions.assertEquals(
        List.of(8, 9), response.records.stream().map(record -> record.value).collect(toList()));

    // close consumer
    response.onComplete(null);
  }

  @Test
  void testDistanceFromBeginning() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    produceData(topic, 10);

    var handler = getRecordHandler();
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.GetResponse.class,
            handler
                .get(
                    Channel.ofQueries(
                        topic, Map.of(DISTANCE_FROM_BEGINNING, "8", VALUE_DESERIALIZER, "integer")))
                .toCompletableFuture()
                .get());

    Assertions.assertEquals(2, response.records.size());
    Assertions.assertEquals(
        List.of(8, 9), response.records.stream().map(record -> record.value).collect(toList()));

    // close consumer
    response.onComplete(null);
  }

  @Test
  void testSeekTo() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    produceData(topic, 5);

    var handler = getRecordHandler();
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.GetResponse.class,
            handler
                .get(Channel.ofQueries(topic, Map.of(SEEK_TO, "3", VALUE_DESERIALIZER, "integer")))
                .toCompletableFuture()
                .get());

    Assertions.assertEquals(2, response.records.size());
    Assertions.assertEquals(
        List.of(3, 4), response.records.stream().map(record -> record.value).collect(toList()));

    // close consumer
    response.onComplete(null);
  }

  @Test
  void testGetRecordByPartition() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    try (var admin = AsyncAdmin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      var partitionNum = 2;
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(partitionNum)
          .run()
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));

      for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
        for (int recordIdx = 0; recordIdx < 10; recordIdx++) {
          producer
              .sender()
              .topic(topic)
              .partition(partitionId)
              .value(ByteBuffer.allocate(4).putInt(recordIdx).array())
              .run();
        }
      }
      producer.flush();
    }

    var handler = getRecordHandler();
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.GetResponse.class,
            handler
                .get(Channel.ofQueries(topic, Map.of(DISTANCE_FROM_BEGINNING, "1", PARTITION, "1")))
                .toCompletableFuture()
                .get());

    Assertions.assertTrue(
        response.records.stream().map(r -> r.partition).filter(p -> p != 1).findAny().isEmpty());

    // close consumer
    response.onComplete(null);
  }

  @Test
  void testLimit() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    produceData(topic, 10);

    var handler = getRecordHandler();
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.GetResponse.class,
            handler
                .get(
                    Channel.ofQueries(
                        topic,
                        Map.of(
                            DISTANCE_FROM_BEGINNING,
                            "2",
                            LIMIT,
                            "3",
                            VALUE_DESERIALIZER,
                            "integer")))
                .toCompletableFuture()
                .get());

    // limit is just a recommended size here, we might get more records than limit
    Assertions.assertEquals(8, response.records.size());
    Assertions.assertEquals(
        List.of(2, 3, 4, 5, 6, 7, 8, 9),
        response.records.stream().map(record -> record.value).collect(toList()));

    // close consumer
    response.onComplete(null);
  }

  @ParameterizedTest
  @MethodSource("forTestDeserializer")
  void testDeserializer(String valueDeserializer, byte[] value, Object expectedValue)
      throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    try (var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      producer.sender().topic(topic).value(value).run();
      producer.flush();
    }

    var handler = getRecordHandler();
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.GetResponse.class,
            handler
                .get(
                    Channel.ofQueries(
                        topic,
                        Map.of(DISTANCE_FROM_LATEST, "1", VALUE_DESERIALIZER, valueDeserializer)))
                .toCompletableFuture()
                .get());
    var records = List.copyOf(response.records);
    Assertions.assertEquals(1, records.size());

    if (valueDeserializer.equals("bytearray")) {
      Assertions.assertArrayEquals((byte[]) expectedValue, (byte[]) records.get(0).value);
    } else {
      Assertions.assertEquals(expectedValue, records.get(0).value);
    }

    // close consumer
    response.onComplete(null);
  }

  private static Stream<Arguments> forTestDeserializer() {
    return Stream.of(
        arguments("bytearray", "astraea".getBytes(UTF_8), "astraea".getBytes(UTF_8)),
        arguments("string", "astraea".getBytes(UTF_8), "astraea"),
        arguments(
            "integer",
            ByteBuffer.allocate(Integer.BYTES).putInt(Integer.MIN_VALUE).array(),
            Integer.MIN_VALUE),
        arguments(
            "float",
            ByteBuffer.allocate(Float.BYTES).putFloat(Float.MIN_VALUE).array(),
            Float.MIN_VALUE),
        arguments(
            "long",
            ByteBuffer.allocate(Long.BYTES).putLong(Integer.MIN_VALUE - 1L).array(),
            Integer.MIN_VALUE - 1L),
        arguments(
            "double",
            ByteBuffer.allocate(Double.BYTES).putDouble(Float.MAX_VALUE - 1d).array(),
            Float.MAX_VALUE - 1d));
  }

  @Test
  void testGetResponse() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    var timestamp = System.currentTimeMillis();
    try (var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      producer
          .sender()
          .topic(topic)
          .key("astraea".getBytes(UTF_8))
          .value(ByteBuffer.allocate(Integer.BYTES).putInt(100).array())
          .headers(List.of(Header.of("a", "b".getBytes(UTF_8))))
          .timestamp(timestamp)
          .run();
      producer.flush();
    }
    var handler = getRecordHandler();
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.GetResponse.class,
            handler
                .get(
                    Channel.ofQueries(
                        topic,
                        Map.of(
                            DISTANCE_FROM_LATEST,
                            "1",
                            KEY_DESERIALIZER,
                            "string",
                            VALUE_DESERIALIZER,
                            "integer")))
                .toCompletableFuture()
                .get());
    Assertions.assertEquals(1, response.records.size());
    var recordDto = response.records.iterator().next();
    Assertions.assertEquals(topic, recordDto.topic);
    Assertions.assertEquals(0, recordDto.partition);
    Assertions.assertEquals(0, recordDto.offset);
    Assertions.assertEquals(timestamp, recordDto.timestamp);
    Assertions.assertEquals("astraea", recordDto.key);
    Assertions.assertEquals(100, recordDto.value);
    Assertions.assertEquals("astraea".getBytes(UTF_8).length, recordDto.serializedKeySize);
    Assertions.assertEquals(Integer.BYTES, recordDto.serializedValueSize);
    Assertions.assertEquals(0, recordDto.leaderEpoch);

    Assertions.assertEquals(1, recordDto.headers.size());
    var headerDto = recordDto.headers.iterator().next();
    Assertions.assertEquals("a", headerDto.key);
    Assertions.assertArrayEquals("b".getBytes(UTF_8), headerDto.value);

    // close consumer
    response.onComplete(null);
  }

  @Test
  void testGetJsonResponse() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    var timestamp = System.currentTimeMillis();
    try (var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      producer
          .sender()
          .topic(topic)
          .key("astraea".getBytes())
          .value(ByteBuffer.allocate(Integer.BYTES).putInt(100).array())
          .headers(List.of(Header.of("a", null)))
          .timestamp(timestamp)
          .run();
      producer.flush();
    }
    var handler = getRecordHandler();
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.GetResponse.class,
            handler
                .get(
                    Channel.ofQueries(
                        topic,
                        Map.of(
                            DISTANCE_FROM_LATEST,
                            "1",
                            KEY_DESERIALIZER,
                            "bytearray",
                            VALUE_DESERIALIZER,
                            "integer")))
                .toCompletableFuture()
                .get());

    Assertions.assertEquals(
        "{\"records\":[{"
            + "\"topic\":\""
            + topic
            + "\","
            + "\"partition\":0,"
            + "\"offset\":0,"
            + "\"timestamp\":"
            + timestamp
            + ","
            + "\"serializedKeySize\":7,"
            + "\"serializedValueSize\":4,"
            + "\"headers\":[{\"key\":\"a\"}],"
            + "\"key\":\""
            + Base64.getEncoder().encodeToString("astraea".getBytes(UTF_8))
            + "\","
            + "\"value\":100,"
            + "\"leaderEpoch\":0"
            + "}]}",
        response.json());

    // close consumer
    response.onComplete(null);
  }

  @Test
  void testByteArrayToBase64TypeAdapter() {
    var foo = new Foo("test".getBytes());
    var gson =
        new GsonBuilder()
            .registerTypeHierarchyAdapter(byte[].class, new ByteArrayToBase64TypeAdapter())
            .create();
    Assertions.assertArrayEquals(foo.bar, gson.fromJson(gson.toJson(foo), Foo.class).bar);
  }

  private static class Foo {
    final byte[] bar;

    public Foo(byte[] bar) {
      this.bar = bar;
    }
  }

  @Test
  void testPostAndGet() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    var handler = getRecordHandler();
    var currentTimestamp = System.currentTimeMillis();
    Assertions.assertInstanceOf(
        RecordHandler.PostResponse.class,
        handler
            .post(
                Channel.ofRequest(
                    PostRequest.of(
                        new Gson()
                            .toJson(
                                Map.of(
                                    RECORDS,
                                    List.of(
                                        new RecordHandler.PostRecord(
                                            topic,
                                            0,
                                            "string",
                                            "integer",
                                            "foo",
                                            "100",
                                            currentTimestamp)))))))
            .toCompletableFuture()
            .get());

    var response =
        Assertions.assertInstanceOf(
            RecordHandler.GetResponse.class,
            handler
                .get(
                    Channel.ofQueries(
                        topic,
                        Map.of(
                            DISTANCE_FROM_LATEST,
                            "1",
                            KEY_DESERIALIZER,
                            "string",
                            VALUE_DESERIALIZER,
                            "integer",
                            PARTITION,
                            "0")))
                .toCompletableFuture()
                .get());
    var record = response.records.iterator().next();
    Assertions.assertEquals(topic, record.topic);
    Assertions.assertEquals(0, record.partition);
    Assertions.assertEquals(0, record.offset);
    Assertions.assertEquals(0, record.leaderEpoch);
    Assertions.assertEquals("foo".getBytes().length, record.serializedKeySize);
    Assertions.assertEquals(4, record.serializedValueSize);
    Assertions.assertEquals("foo", record.key);
    Assertions.assertEquals(100, record.value);
    Assertions.assertEquals(currentTimestamp, record.timestamp);
    Assertions.assertEquals(List.of(), record.headers);

    // close consumer
    response.onComplete(null);
  }

  @Test
  void testTimeout() throws ExecutionException, InterruptedException {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> getRecordHandler().get(Channel.ofQueries("test", Map.of(TIMEOUT, "foo"))));
    var response =
        getRecordHandler()
            .get(Channel.ofQueries("test", Map.of(TIMEOUT, "10s")))
            .toCompletableFuture()
            .get();
    Assertions.assertInstanceOf(RecordHandler.GetResponse.class, response);
    // close consumer
    response.onComplete(null);
  }

  @Test
  void testDeleteParameter() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var topicName = Utils.randomString(10);
      var handler = getRecordHandler();
      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .get();
      Utils.sleep(Duration.ofSeconds(2));
      Assertions.assertEquals(
          Response.OK,
          handler
              .delete(Channel.ofQueries(topicName, Map.of(PARTITION, "0", OFFSET, "0")))
              .toCompletableFuture()
              .get());
      Assertions.assertEquals(
          Response.OK,
          handler
              .delete(Channel.ofQueries(topicName, Map.of(OFFSET, "0")))
              .toCompletableFuture()
              .get());
      Assertions.assertEquals(
          Response.OK,
          handler
              .delete(Channel.ofQueries(topicName, Map.of(PARTITION, "0")))
              .toCompletableFuture()
              .get());
      Assertions.assertEquals(
          Response.OK, handler.delete(Channel.ofTarget(topicName)).toCompletableFuture().get());
    }
  }

  @Test
  void testDelete() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      var topicName = Utils.randomString(10);
      var handler = getRecordHandler();
      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .get();

      var senders =
          Stream.of(0, 0, 1, 1, 1, 2, 2, 2, 2)
              .map(x -> producer.sender().topic(topicName).partition(x).value(new byte[100]))
              .collect(Collectors.toList());
      producer.send(senders);
      producer.flush();

      Assertions.assertEquals(
          Response.OK,
          handler
              .delete(Channel.ofQueries(topicName, Map.of(PARTITION, "0", OFFSET, "1")))
              .toCompletableFuture()
              .get());
      Utils.sleep(Duration.ofSeconds(2));
      var partitions = admin.partitions(Set.of(topicName)).toCompletableFuture().get();
      Assertions.assertEquals(3, partitions.size());
      Assertions.assertEquals(
          1,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 0)
              .findFirst()
              .get()
              .earliestOffset());
      Assertions.assertEquals(
          0,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 1)
              .findFirst()
              .get()
              .earliestOffset());
      Assertions.assertEquals(
          0,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 2)
              .findFirst()
              .get()
              .earliestOffset());

      Assertions.assertEquals(
          Response.OK, handler.delete(Channel.ofTarget(topicName)).toCompletableFuture().get());
      partitions =
          admin
              .partitions(admin.topicNames(true).toCompletableFuture().get())
              .toCompletableFuture()
              .get();
      Assertions.assertEquals(
          2,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 0)
              .findFirst()
              .get()
              .earliestOffset());
      Assertions.assertEquals(
          3,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 1)
              .findFirst()
              .get()
              .earliestOffset());
      Assertions.assertEquals(
          4,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 2)
              .findFirst()
              .get()
              .earliestOffset());
    }
  }

  @Test
  void testDeleteOffset() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      var topicName = Utils.randomString(10);
      var handler = getRecordHandler();
      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .get();

      var senders =
          Stream.of(0, 0, 1, 1, 1, 2, 2, 2, 2)
              .map(x -> producer.sender().topic(topicName).partition(x).value(new byte[100]))
              .collect(Collectors.toList());
      producer.send(senders);
      producer.flush();

      Assertions.assertEquals(
          Response.OK,
          handler
              .delete(Channel.ofQueries(topicName, Map.of(OFFSET, "1")))
              .toCompletableFuture()
              .get());
      var partitions =
          admin
              .partitions(admin.topicNames(true).toCompletableFuture().get())
              .toCompletableFuture()
              .get();
      Assertions.assertEquals(
          1,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 0)
              .findFirst()
              .get()
              .earliestOffset());
      Assertions.assertEquals(
          1,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 1)
              .findFirst()
              .get()
              .earliestOffset());
      Assertions.assertEquals(
          1,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 2)
              .findFirst()
              .get()
              .earliestOffset());
    }
  }

  @Test
  void testDeletePartition() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      var topicName = Utils.randomString(10);
      var handler = getRecordHandler();
      admin
          .creator()
          .topic(topicName)
          .numberOfPartitions(3)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .get();

      var senders =
          Stream.of(0, 0, 1, 1, 1, 2, 2, 2, 2)
              .map(x -> producer.sender().topic(topicName).partition(x).value(new byte[100]))
              .collect(Collectors.toList());
      producer.send(senders);
      producer.flush();

      Assertions.assertEquals(
          Response.OK,
          handler
              .delete(Channel.ofQueries(topicName, Map.of(PARTITION, "1")))
              .toCompletableFuture()
              .get());
      var partitions =
          admin
              .partitions(admin.topicNames(true).toCompletableFuture().get())
              .toCompletableFuture()
              .get();
      Assertions.assertEquals(
          0,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 0)
              .findFirst()
              .get()
              .earliestOffset());
      Assertions.assertEquals(
          3,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 1)
              .findFirst()
              .get()
              .earliestOffset());
      Assertions.assertEquals(
          0,
          partitions.stream()
              .filter(p -> p.topic().equals(topicName) && p.partition() == 2)
              .findFirst()
              .get()
              .earliestOffset());
    }
  }

  @Test
  void testGetRecordsCommitOffsetWithGroupId() {
    var topic = Utils.randomString(10);
    var groupId = Utils.randomString(10);
    var recordHandler = getRecordHandler();

    Function<Boolean, RecordHandler.GetResponse> getRecords =
        needError ->
            Assertions.assertInstanceOf(
                RecordHandler.GetResponse.class,
                Utils.packException(
                    () ->
                        recordHandler
                            .handle(
                                Channel.builder()
                                    .type(Channel.Type.GET)
                                    .target(topic)
                                    .queries(
                                        Map.of(GROUP_ID, groupId, VALUE_DESERIALIZER, "integer"))
                                    .sender(
                                        r -> {
                                          if (needError) throw new RuntimeException();
                                        })
                                    .build())
                            .toCompletableFuture()
                            .get()));

    // send this request to register consumer group
    Assertions.assertEquals(getRecords.apply(false).records.size(), 0);

    produceData(topic, 5);
    Assertions.assertEquals(getRecords.apply(false).records.size(), 5);

    // can't send data to caller, so the offsets are not committed
    produceData(topic, 2);
    Assertions.assertEquals(getRecords.apply(true).records.size(), 2);

    // ok, offsets are committed
    Assertions.assertEquals(getRecords.apply(false).records.size(), 2);

    // all offsets are committed, so this group id can't get more data
    Assertions.assertEquals(getRecords.apply(false).records.size(), 0);
  }

  // test consumer in different modes, subscribe and assignment
  private static Stream<Arguments> forTestGetRecordsCloseConsumer() {
    return Stream.of(
        arguments(Map.of(GROUP_ID, Utils.randomString(10))), arguments(Map.of(PARTITION, "0")));
  }

  @ParameterizedTest
  @MethodSource("forTestGetRecordsCloseConsumer")
  void testGetRecordsCloseConsumer(Map<String, String> args)
      throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    var recordHandler = getRecordHandler();

    var response =
        Assertions.assertInstanceOf(
            RecordHandler.GetResponse.class,
            recordHandler.handle(Channel.ofQueries(topic, args)).toCompletableFuture().get());
    var error =
        Assertions.assertThrows(
            IllegalStateException.class, () -> response.consumer.poll(Duration.ofSeconds(1)));
    Assertions.assertEquals(error.getMessage(), "This consumer has already been closed.");

    var response2 =
        Assertions.assertInstanceOf(
            RecordHandler.GetResponse.class,
            recordHandler
                .handle(
                    Channel.builder()
                        .type(Channel.Type.GET)
                        .target(topic)
                        .queries(args)
                        .sender(
                            ignored -> {
                              throw new RuntimeException();
                            })
                        .build())
                .toCompletableFuture()
                .get());
    var error2 =
        Assertions.assertThrows(
            IllegalStateException.class, () -> response2.consumer.poll(Duration.ofSeconds(1)));
    Assertions.assertEquals(error2.getMessage(), "This consumer has already been closed.");
  }

  private RecordHandler getRecordHandler() {
    return new RecordHandler(AsyncAdmin.of(bootstrapServers()), bootstrapServers());
  }

  @Test
  void testRecords() {
    var exception = new IllegalArgumentException("hello");
    @SuppressWarnings({"unchecked", "resource"})
    Consumer<byte[], byte[]> consumer = Mockito.mock(Consumer.class);
    Mockito.when(consumer.poll(Mockito.anyInt(), Mockito.any())).thenThrow(exception);
    var recordHandler = getRecordHandler();
    Assertions.assertEquals(
        exception,
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> recordHandler.get(consumer, 100, Duration.ofSeconds(3))));
  }
}
