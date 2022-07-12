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
import static org.astraea.app.web.RecordHandler.KEY;
import static org.astraea.app.web.RecordHandler.KEY_DESERIALIZER;
import static org.astraea.app.web.RecordHandler.KEY_SERIALIZER;
import static org.astraea.app.web.RecordHandler.PARTITION;
import static org.astraea.app.web.RecordHandler.RECORDS;
import static org.astraea.app.web.RecordHandler.SEEK_TO;
import static org.astraea.app.web.RecordHandler.TIMEOUT;
import static org.astraea.app.web.RecordHandler.TIMESTAMP;
import static org.astraea.app.web.RecordHandler.TOPIC;
import static org.astraea.app.web.RecordHandler.VALUE;
import static org.astraea.app.web.RecordHandler.VALUE_DESERIALIZER;
import static org.astraea.app.web.RecordHandler.VALUE_SERIALIZER;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.gson.GsonBuilder;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.app.admin.Admin;
import org.astraea.app.common.Utils;
import org.astraea.app.consumer.Consumer;
import org.astraea.app.consumer.Header;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireBrokerCluster;
import org.astraea.app.web.RecordHandler.ByteArrayToBase64TypeAdapter;
import org.astraea.app.web.RecordHandler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RecordHandlerTest extends RequireBrokerCluster {

  @Test
  void testInvalidPost() {
    var handler = new RecordHandler(bootstrapServers());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> handler.post(PostRequest.of(Map.of())),
        "topic must be set");
  }

  @Test
  void testPost() {
    var topic = Utils.randomString(10);
    var handler = new RecordHandler(bootstrapServers());
    var currentTimestamp = System.currentTimeMillis();
    var result =
        Assertions.assertInstanceOf(
            Metadata.class,
            handler.post(
                PostRequest.of(
                    Map.of(
                        KEY_SERIALIZER, "string",
                        VALUE_SERIALIZER, "integer",
                        TOPIC, topic,
                        KEY, "foo",
                        VALUE, "100",
                        TIMESTAMP, "" + currentTimestamp,
                        PARTITION, "0"))));

    Assertions.assertEquals(0, result.offset);
    Assertions.assertEquals(0, result.partition);
    Assertions.assertEquals(topic, result.topic);
    Assertions.assertEquals("foo".getBytes(UTF_8).length, result.serializedKeySize);
    Assertions.assertEquals(4, result.serializedValueSize);

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .fromBeginning()
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

  @Test
  void testPostWithAsync() {
    var topic = Utils.randomString(10);
    var handler = new RecordHandler(bootstrapServers());
    var currentTimestamp = System.currentTimeMillis();
    var result =
        Assertions.assertInstanceOf(
            Response.class,
            handler.post(
                PostRequest.of(
                    Map.of(
                        KEY_SERIALIZER, "string",
                        VALUE_SERIALIZER, "integer",
                        TOPIC, topic,
                        KEY, "foo",
                        VALUE, "100",
                        TIMESTAMP, "" + currentTimestamp,
                        ASYNC, "true",
                        PARTITION, "0"))));
    Assertions.assertEquals(Response.ACCEPT, result);

    handler.producer.flush();

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .fromBeginning()
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
  void testSerializer(String serializer, String actual, byte[] expected) {
    var topic = Utils.randomString(10);
    var handler = new RecordHandler(bootstrapServers());
    Assertions.assertInstanceOf(
        Metadata.class,
        handler.post(
            PostRequest.of(Map.of(KEY_SERIALIZER, serializer, TOPIC, topic, KEY, actual))));

    try (var consumer =
        Consumer.forTopics(Set.of(topic))
            .bootstrapServers(bootstrapServers())
            .fromBeginning()
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
  void testInvalidGet() {
    var handler = new RecordHandler(bootstrapServers());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> handler.get(Optional.empty(), Map.of()),
        "topic must be set");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            handler.get(
                Optional.of("topic"),
                Map.of(DISTANCE_FROM_BEGINNING, "1", DISTANCE_FROM_LATEST, "1")),
        "only one seek strategy is allowed");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            handler.get(
                Optional.of("topic"),
                Map.of(DISTANCE_FROM_BEGINNING, "1", DISTANCE_FROM_LATEST, "1", SEEK_TO, "1")),
        "only one seek strategy is allowed");
  }

  @Test
  void testDistanceFromLatest() {
    var topic = Utils.randomString(10);
    produceData(topic, 10);

    var handler = new RecordHandler(bootstrapServers());
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.Records.class,
            handler.get(
                Optional.of(topic),
                Map.of(DISTANCE_FROM_LATEST, "2", VALUE_DESERIALIZER, "integer")));

    Assertions.assertEquals(1, response.data.size());
    Assertions.assertEquals(
        List.of(8), response.data.stream().map(record -> record.value).collect(toList()));
  }

  @Test
  void testDistanceFromBeginning() {
    var topic = Utils.randomString(10);
    produceData(topic, 10);

    var handler = new RecordHandler(bootstrapServers());
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.Records.class,
            handler.get(
                Optional.of(topic),
                Map.of(DISTANCE_FROM_BEGINNING, "8", VALUE_DESERIALIZER, "integer")));

    Assertions.assertEquals(1, response.data.size());
    Assertions.assertEquals(
        List.of(8), response.data.stream().map(record -> record.value).collect(toList()));
  }

  @Test
  void testSeekTo() {
    var topic = Utils.randomString(10);
    produceData(topic, 5);

    var handler = new RecordHandler(bootstrapServers());
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.Records.class,
            handler.get(Optional.of(topic), Map.of(SEEK_TO, "3", VALUE_DESERIALIZER, "integer")));

    Assertions.assertEquals(1, response.data.size());
    Assertions.assertEquals(
        List.of(3), response.data.stream().map(record -> record.value).collect(toList()));
  }

  @Test
  void testGetRecordByPartition() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers());
        var producer = Producer.of(bootstrapServers())) {
      var partitionNum = 2;
      admin.creator().topic(topic).numberOfPartitions(partitionNum).create();
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

    var handler = new RecordHandler(bootstrapServers());
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.Records.class,
            handler.get(Optional.of(topic), Map.of(DISTANCE_FROM_BEGINNING, "1", PARTITION, "1")));

    Assertions.assertEquals(
        List.of(1), response.data.stream().map(r -> r.partition).collect(toList()));
  }

  @Test
  void testRecordNum() {
    var topic = Utils.randomString(10);
    produceData(topic, 10);

    var handler = new RecordHandler(bootstrapServers());
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.Records.class,
            handler.get(
                Optional.of(topic),
                Map.of(DISTANCE_FROM_BEGINNING, "2", RECORDS, "3", VALUE_DESERIALIZER, "integer")));

    Assertions.assertEquals(3, response.data.size());
    Assertions.assertEquals(
        List.of(2, 3, 4), response.data.stream().map(record -> record.value).collect(toList()));
  }

  @ParameterizedTest
  @MethodSource("forTestDeserializer")
  void testDeserializer(String valueDeserializer, byte[] value, Object expectedValue) {
    var topic = Utils.randomString(10);
    try (var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      producer.sender().topic(topic).value(value).run();
      producer.flush();
    }

    var handler = new RecordHandler(bootstrapServers());
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.Records.class,
            handler.get(
                Optional.of(topic),
                Map.of(DISTANCE_FROM_LATEST, "1", VALUE_DESERIALIZER, valueDeserializer)));
    var records = List.copyOf(response.data);
    Assertions.assertEquals(1, records.size());

    if (valueDeserializer.equals("bytearray")) {
      Assertions.assertArrayEquals((byte[]) expectedValue, (byte[]) records.get(0).value);
    } else {
      Assertions.assertEquals(expectedValue, records.get(0).value);
    }
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
  void testGetResponse() {
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
    var handler = new RecordHandler(bootstrapServers());
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.Records.class,
            handler.get(
                Optional.of(topic),
                Map.of(
                    DISTANCE_FROM_LATEST,
                    "1",
                    KEY_DESERIALIZER,
                    "string",
                    VALUE_DESERIALIZER,
                    "integer")));
    Assertions.assertEquals(1, response.data.size());
    var recordDto = response.data.iterator().next();
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
  }

  @Test
  void testGetJsonResponse() {
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
    var handler = new RecordHandler(bootstrapServers());
    var response =
        Assertions.assertInstanceOf(
            RecordHandler.Records.class,
            handler.get(
                Optional.of(topic),
                Map.of(
                    DISTANCE_FROM_LATEST,
                    "1",
                    KEY_DESERIALIZER,
                    "bytearray",
                    VALUE_DESERIALIZER,
                    "integer")));

    Assertions.assertEquals(
        "{\"data\":[{"
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
  void testPostAndGet() {
    var topic = Utils.randomString(10);
    var handler = new RecordHandler(bootstrapServers());
    var currentTimestamp = System.currentTimeMillis();
    Assertions.assertInstanceOf(
        Metadata.class,
        handler.post(
            PostRequest.of(
                Map.of(
                    KEY_SERIALIZER, "string",
                    VALUE_SERIALIZER, "integer",
                    TOPIC, topic,
                    KEY, "foo",
                    VALUE, "100",
                    TIMESTAMP, "" + currentTimestamp,
                    PARTITION, "0"))));

    var records =
        Assertions.assertInstanceOf(
            RecordHandler.Records.class,
            handler.get(
                Optional.of(topic),
                Map.of(
                    DISTANCE_FROM_LATEST,
                    "1",
                    KEY_DESERIALIZER,
                    "string",
                    VALUE_DESERIALIZER,
                    "integer",
                    PARTITION,
                    "0")));
    var record = records.data.iterator().next();
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
  }

  @Test
  void testTimeout() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new RecordHandler(bootstrapServers()).get(Optional.of("test"), Map.of(TIMEOUT, "foo")));
    Assertions.assertInstanceOf(
        RecordHandler.Records.class,
        new RecordHandler(bootstrapServers()).get(Optional.of("test"), Map.of(TIMEOUT, "10s")));
  }
}
