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
    Assertions.assertEquals("10", PostRequest.handleDouble(10.00));
    Assertions.assertEquals("10.01", PostRequest.handleDouble(10.01));
    Assertions.assertEquals("xxxx", PostRequest.handleDouble("xxxx"));
    Assertions.assertEquals("{\"foo\":1}", PostRequest.handleDouble(Map.of("foo", 1)));
    Assertions.assertEquals("[\"a\",1]", PostRequest.handleDouble(List.of("a", 1)));
    Assertions.assertEquals(
        "[{\"foo\":{\"bar\":10}}]",
        PostRequest.handleDouble(List.of(Map.of("foo", Map.of("bar", 10)))));
    Assertions.assertEquals(
        "[{\"foo\":{\"bar\":1.1}}]",
        PostRequest.handleDouble(List.of(Map.of("foo", Map.of("bar", 1.1d)))));
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
    Assertions.assertEquals(2, request.ints("c").size());
    Assertions.assertEquals(1, request.ints("c").get(0));
    Assertions.assertEquals(2, request.ints("c").get(1));
  }
}
