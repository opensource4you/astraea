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

import com.sun.net.httpserver.HttpExchange;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PostRequestTest {

  @Test
  void testParseHttpExchange() throws IOException {
    var input =
        new ByteArrayInputStream("{\"a\":\"b\",\"c\":123}".getBytes(StandardCharsets.UTF_8));
    var exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getRequestBody()).thenReturn(input);

    var request = PostRequest.of(exchange);
    Assertions.assertEquals(2, request.raw().size());
    Assertions.assertEquals("b", request.raw().get("a"));
    Assertions.assertEquals(123, request.intValue("c"));
  }

  @Test
  void testHandleDouble() {
    Assertions.assertEquals("10", PostRequest.handle(10.00));
    Assertions.assertEquals("10.01", PostRequest.handle(10.01));
    Assertions.assertEquals("xxxx", PostRequest.handle("xxxx"));
    Assertions.assertEquals("{\"foo\":1}", PostRequest.handle(Map.of("foo", 1)));
    Assertions.assertEquals("[\"a\",1]", PostRequest.handle(List.of("a", 1)));
    Assertions.assertEquals(
        "[{\"foo\":{\"bar\":10}}]", PostRequest.handle(List.of(Map.of("foo", Map.of("bar", 10)))));
    Assertions.assertEquals(
        "[{\"foo\":{\"bar\":1.1}}]",
        PostRequest.handle(List.of(Map.of("foo", Map.of("bar", 1.1d)))));
  }

  @Test
  void testParseJson() {
    var request = PostRequest.of("{\"a\":1234, \"b\":3.34}");
    Assertions.assertEquals(1234, request.intValue("a"));
    Assertions.assertEquals(1234, request.shortValue("a"));
    Assertions.assertEquals(1234.0, request.shortValue("a"));
    Assertions.assertEquals(3.34, request.doubleValue("b"));
  }

  @Test
  void testStringArray() throws IOException {
    var input =
        new ByteArrayInputStream(
            "{\"a\":\"b\",\"c\":[\"1\",\"2\"]}".getBytes(StandardCharsets.UTF_8));
    var exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getRequestBody()).thenReturn(input);

    var request = PostRequest.of(exchange);
    Assertions.assertEquals(2, request.values("c").size());
    Assertions.assertEquals("1", request.values("c").get(0));
    Assertions.assertEquals("2", request.values("c").get(1));
  }

  @Test
  void testIntegerArray() throws IOException {
    var input =
        new ByteArrayInputStream("{\"a\":\"b\",\"c\":[1,2]}".getBytes(StandardCharsets.UTF_8));
    var exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getRequestBody()).thenReturn(input);

    var request = PostRequest.of(exchange);
    Assertions.assertEquals(2, request.intValues("c").size());
    Assertions.assertEquals(1, request.intValues("c").get(0));
    Assertions.assertEquals(2, request.intValues("c").get(1));
  }

  @Test
  void testValues() throws IOException {
    var input =
        new ByteArrayInputStream(
            "{\"a\":[{\"foo\": \"r1\", \"bar\": 1},{\"foo\": \"r2\", \"bar\": 2}]}"
                .getBytes(StandardCharsets.UTF_8));
    var exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getRequestBody()).thenReturn(input);

    var request = PostRequest.of(exchange);
    Assertions.assertEquals(
        List.of(new ForTestValue("r1", 1), new ForTestValue("r2", 2)),
        request.values("a", ForTestValue.class));
  }

  @Test
  void testValue() throws IOException {
    var input =
        new ByteArrayInputStream(
            "{\"a\":{\"foo\": \"r1\", \"bar\": 1}}".getBytes(StandardCharsets.UTF_8));
    var exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getRequestBody()).thenReturn(input);

    var request = PostRequest.of(exchange);
    Assertions.assertEquals(new ForTestValue("r1", 1), request.value("a", ForTestValue.class));
  }

  @Test
  void testRequests() {
    var data = "{\"key\":[{\"a\":\"aa\",\"b\":2}, {\"c\":[\"a\", \"bbb\"]}]}";
    var elements = PostRequest.of(data).requests("key");
    Assertions.assertEquals(2, elements.size());
    Assertions.assertEquals("aa", elements.get(0).value("a"));
    Assertions.assertEquals(2, elements.get(0).intValue("b"));
    Assertions.assertEquals(List.of("a", "bbb"), elements.get(1).values("c"));
  }

  @Test
  void testStrings() {
    var data = "{\"key\": \"v\",\"key2\": [\"v2\", \"v3\"]}";
    Assertions.assertEquals("v", PostRequest.of(data).value("key"));
    Assertions.assertEquals(List.of("v2", "v3"), PostRequest.of(data).values("key2"));
    Assertions.assertEquals(Optional.empty(), PostRequest.of(data).get("nonexistent"));
    Assertions.assertThrows(
        NoSuchElementException.class, () -> PostRequest.of(data).value("nonexistent"));
  }

  @Test
  void testShort() {
    var data = "{\"key\":1, \"key2\": [1, " + Short.MAX_VALUE + "]}";
    Assertions.assertEquals(1, PostRequest.of(data).shortValue("key"));
    Assertions.assertEquals(
        List.of((short) 1, Short.MAX_VALUE), PostRequest.of(data).shortValues("key2"));
    Assertions.assertEquals(Optional.empty(), PostRequest.of(data).getShort("nonexistent"));
    Assertions.assertThrows(
        NoSuchElementException.class, () -> PostRequest.of(data).shortValue("nonexistent"));
  }

  @Test
  void testInt() {
    var data = "{\"key\":1, \"key2\": [1, " + Integer.MAX_VALUE + "]}";
    Assertions.assertEquals(1, PostRequest.of(data).intValue("key"));
    Assertions.assertEquals(List.of(1, Integer.MAX_VALUE), PostRequest.of(data).intValues("key2"));
    Assertions.assertEquals(Optional.empty(), PostRequest.of(data).getInt("nonexistent"));
    Assertions.assertThrows(
        NoSuchElementException.class, () -> PostRequest.of(data).intValue("nonexistent"));
  }

  @Test
  void testLong() {
    var data = "{\"key\":1, \"key2\": [1, " + Long.MAX_VALUE + "]}";
    Assertions.assertEquals(1, PostRequest.of(data).longValue("key"));
    Assertions.assertEquals(List.of(1L, Long.MAX_VALUE), PostRequest.of(data).longValues("key2"));
    Assertions.assertEquals(Optional.empty(), PostRequest.of(data).getLong("nonexistent"));
    Assertions.assertThrows(
        NoSuchElementException.class, () -> PostRequest.of(data).longValue("nonexistent"));
  }

  @Test
  void testDouble() {
    var data = "{\"key\":1, \"key2\": [1, " + Double.MAX_VALUE + "]}";
    Assertions.assertEquals(1, PostRequest.of(data).doubleValue("key"));
    Assertions.assertEquals(
        List.of(1D, Double.MAX_VALUE), PostRequest.of(data).doubleValues("key2"));
    Assertions.assertEquals(Optional.empty(), PostRequest.of(data).getDouble("nonexistent"));
    Assertions.assertThrows(
        NoSuchElementException.class, () -> PostRequest.of(data).doubleValue("nonexistent"));
  }

  @Test
  void testHas() {
    var data = "{\"key\":1, \"key2\": \"a\"}";
    Assertions.assertTrue(PostRequest.of(data).has("key"));
    Assertions.assertTrue(PostRequest.of(data).has("key", "key2"));
    Assertions.assertFalse(PostRequest.of(data).has("key", "key2", "key3"));
    Assertions.assertFalse(PostRequest.of(data).has("xxx"));
  }

  static class ForTestValue {
    final String foo;
    final Integer bar;

    ForTestValue(String foo, Integer bar) {
      this.foo = foo;
      this.bar = bar;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if ((obj == null) || (getClass() != obj.getClass())) {
        return false;
      }
      ForTestValue other = (ForTestValue) obj;
      return Objects.equals(foo, other.foo) && Objects.equals(bar, other.bar);
    }
  }
}
