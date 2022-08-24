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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class HandlerTest {

  @Test
  void testException() {
    var exception = new IllegalStateException("hello");
    Handler handler =
        (paths, queries) -> {
          throw exception;
        };

    var exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getRequestURI()).thenReturn(URI.create("http://localhost:8888/abc"));
    Mockito.when(exchange.getRequestMethod()).thenReturn("get");
    var r = Assertions.assertInstanceOf(Response.ResponseImpl.class, handler.process(exchange));
    Assertions.assertNotEquals(200, r.code);
    Assertions.assertEquals(exception.getMessage(), r.message);
  }

  @Test
  void testParseTarget() {
    Assertions.assertFalse(
        Handler.parseTarget(URI.create("http://localhost:11111/abc")).isPresent());
    var target = Handler.parseTarget(URI.create("http://localhost:11111/abc/bbb"));
    Assertions.assertTrue(target.isPresent());
    Assertions.assertEquals("bbb", target.get());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Handler.parseTarget(URI.create("http://localhost:11111/abc/bbb/dddd")));
  }

  @Test
  void testParseQuery() {
    var uri = URI.create("http://localhost:11111/abc?k=v&a=b");
    var queries = Handler.parseQueries(uri);
    Assertions.assertEquals(2, queries.size());
    Assertions.assertEquals("v", queries.get("k"));
    Assertions.assertEquals("b", queries.get("a"));
  }

  @Test
  void testNoResponseBody() throws IOException {
    Handler handler =
        new Handler() {
          @Override
          public Response get(Optional<String> target, Map<String, String> queries) {
            return null;
          }

          @Override
          public Response process(HttpExchange exchange) {
            return Response.OK;
          }
        };
    var he = Mockito.mock(HttpExchange.class);
    Mockito.when(he.getResponseHeaders()).thenReturn(Mockito.mock(Headers.class));
    Mockito.when(he.getResponseBody()).thenReturn(Mockito.mock(OutputStream.class));
    handler.handle(he);
    Mockito.verify(he).sendResponseHeaders(200, 0);
  }

  @Test
  void testOnComplete() {
    var response = Mockito.mock(Response.class);
    var exception = new IllegalStateException("hello");
    Mockito.when(response.json()).thenThrow(exception);
    Handler handler = (paths, queries) -> response;

    var exchange = Mockito.mock(HttpExchange.class);
    Mockito.when(exchange.getRequestURI()).thenReturn(URI.create("http://localhost:8888/abc"));
    Mockito.when(exchange.getRequestMethod()).thenReturn("get");

    handler.handle(exchange);
    Mockito.verify(response).onComplete(exception);
  }
}
