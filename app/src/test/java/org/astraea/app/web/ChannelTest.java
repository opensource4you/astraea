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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.astraea.common.json.TypeRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ChannelTest {

  @Test
  void testParse() throws IOException {
    var exchange =
        mock(Channel.Type.GET, URI.create("http://localhost:11111/abc/obj?a=b&c=d"), new byte[0]);
    var channel = Channel.of(exchange);
    Assertions.assertEquals(Channel.Type.GET, channel.type());
    Assertions.assertEquals(Optional.of("obj"), channel.target());
    Assertions.assertEquals(Map.of("a", "b", "c", "d"), channel.queries());
    Assertions.assertEquals(Map.of(), channel.request(TypeRef.map(String.class)));
  }

  @Test
  void testIncorrectTarget() throws IOException {
    var exchange =
        mock(Channel.Type.GET, URI.create("http://localhost:11111/abc/obj/incorrect"), new byte[0]);
    Assertions.assertThrows(IllegalArgumentException.class, () -> Channel.of(exchange));
  }

  @Test
  void testOnComplete() {
    var exception = new IllegalStateException("hello");
    var channel =
        Channel.builder()
            .sender(
                r -> {
                  throw exception;
                })
            .build();
    var response = Mockito.mock(Response.class);
    channel.send(response);
    Mockito.verify(response).onComplete(exception);
  }

  @Test
  void testNoResponseBody() throws IOException {
    var he = mock(Channel.Type.GET, URI.create("http://localhost:11111/abc/obj"), new byte[0]);
    var channel = Channel.of(he);
    channel.send(Response.OK);
    Mockito.verify(he).sendResponseHeaders(200, 0);
  }

  private static HttpExchange mock(Channel.Type type, URI uri, byte[] input) throws IOException {
    var he = Mockito.mock(HttpExchange.class);
    Mockito.when(he.getRequestMethod()).thenReturn(type.name());
    Mockito.when(he.getRequestURI()).thenReturn(uri);
    Mockito.when(he.getResponseHeaders()).thenReturn(Mockito.mock(Headers.class));
    var inputStream = Mockito.mock(InputStream.class);
    Mockito.when(inputStream.readAllBytes()).thenReturn(input);
    Mockito.when(he.getRequestBody()).thenReturn(inputStream);
    Mockito.when(he.getResponseBody()).thenReturn(Mockito.mock(OutputStream.class));
    return he;
  }
}
