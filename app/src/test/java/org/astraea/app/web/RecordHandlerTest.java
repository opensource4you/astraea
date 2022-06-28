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
import static org.astraea.app.web.RecordHandler.ASYNC;
import static org.astraea.app.web.RecordHandler.KEY;
import static org.astraea.app.web.RecordHandler.KEY_SERIALIZER;
import static org.astraea.app.web.RecordHandler.PARTITION;
import static org.astraea.app.web.RecordHandler.TIMESTAMP;
import static org.astraea.app.web.RecordHandler.TOPIC;
import static org.astraea.app.web.RecordHandler.VALUE;
import static org.astraea.app.web.RecordHandler.VALUE_SERIALIZER;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.astraea.app.common.Utils;
import org.astraea.app.consumer.Consumer;
import org.astraea.app.service.RequireBrokerCluster;
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
}
