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
package org.astraea.common.http;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import org.astraea.common.json.JsonSerializationException;
import org.astraea.common.json.TypeRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HttpExecutorTest {
  @Test
  void testGet() {
    try (var server = new Server()) {
      var client = HttpExecutor.builder().build();

      // test get
      server.setRes(200, "{'responseValue':'testValue'}");
      Assertions.assertEquals(
          "testValue",
          client
              .get("http://localhost:" + server.port() + "/test", TypeRef.of(TestResponse.class))
              .toCompletableFuture()
              .join()
              .body()
              .responseValue);

      // check request
      Assertions.assertNotNull(server.req);
      Assertions.assertEquals("get", server.req.method.toLowerCase());
      Assertions.assertEquals("/test", server.req.path);
      Assertions.assertNull(server.req.body);
      Assertions.assertNull(server.req.query);

      // test get with query
      server.setRes(200, "{'responseValue':'testValue'}");
      Assertions.assertEquals(
          "testValue",
          client
              .get(
                  "http://localhost:" + server.port() + "/test",
                  Map.of("a", "b"),
                  TypeRef.of(TestResponse.class))
              .toCompletableFuture()
              .join()
              .body()
              .responseValue);

      // check request
      Assertions.assertNotNull(server.req);
      Assertions.assertEquals("get", server.req.method.toLowerCase());
      Assertions.assertEquals("/test", server.req.path);
      Assertions.assertNull(server.req.body);
      Assertions.assertEquals("a=b", server.req.query);

      // add not found exception
      server.setRes(404, null);
      var exception =
          Assertions.assertInstanceOf(
              HttpRequestException.class,
              Assertions.assertThrows(
                      CompletionException.class,
                      () ->
                          client
                              .get(
                                  "http://localhost:" + server.port() + "/test",
                                  TypeRef.of(TestResponse.class))
                              .toCompletableFuture()
                              .join())
                  .getCause());

      Assertions.assertEquals("get error code: 404", exception.getMessage());
      Assertions.assertEquals(404, exception.code());
    }
  }

  @Test
  void testGetNestedObject() {
    try (var server = new Server()) {
      var client = HttpExecutor.builder().build();
      server.setRes(200, "['v1','v2']");
      Assertions.assertEquals(
          List.of("v1", "v2"),
          client
              .get("http://localhost:" + server.port() + "/test", TypeRef.array(String.class))
              .toCompletableFuture()
              .join()
              .body());
    }
  }

  @Test
  void testPostAndPut() {
    try (var server = new Server()) {
      var client = HttpExecutor.builder().build();

      // test post
      server.setRes(200, "{'responseValue':'testValue'}");
      Assertions.assertEquals(
          "testValue",
          client
              .post(
                  "http://localhost:" + server.port() + "/test",
                  new TestRequest("testRequestValue"),
                  TypeRef.of(TestResponse.class))
              .toCompletableFuture()
              .join()
              .body()
              .responseValue);

      // check post request
      Assertions.assertNotNull(server.req);
      Assertions.assertEquals("post", server.req.method.toLowerCase());
      Assertions.assertEquals("/test", server.req.path);
      Assertions.assertNotNull(server.req.body);

      // set wrong response for post
      server.setRes(200, "{'xxx':'testValue'}");
      Assertions.assertInstanceOf(
          JsonSerializationException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      client
                          .post(
                              "http://localhost:" + server.port() + "/test",
                              new TestRequest("testRequestValue"),
                              TypeRef.of(TestResponse.class))
                          .toCompletableFuture()
                          .join())
              .getCause());
    }
  }

  @Test
  void testPut() {
    try (var server = new Server()) {
      var client = HttpExecutor.builder().build();
      // test put
      server.setRes(200, "{'responseValue':'testValue'}");
      Assertions.assertEquals(
          "testValue",
          client
              .put(
                  "http://localhost:" + server.port() + "/test",
                  new TestRequest("testRequestValue"),
                  TypeRef.of(TestResponse.class))
              .toCompletableFuture()
              .join()
              .body()
              .responseValue);

      // check put request
      Assertions.assertNotNull(server.req);
      Assertions.assertEquals("put", server.req.method.toLowerCase());
      Assertions.assertEquals("/test", server.req.path);
      Assertions.assertNotNull(server.req.body);

      // set wrong response for put
      server.setRes(200, "{'xxx':'testValue'}");
      Assertions.assertInstanceOf(
          JsonSerializationException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      client
                          .put(
                              "http://localhost:" + server.port() + "/test",
                              new TestRequest("testRequestValue"),
                              TypeRef.of(TestResponse.class))
                          .toCompletableFuture()
                          .join())
              .getCause());
    }
  }

  @Test
  void testDelete() {
    try (var server = new Server()) {
      var client = HttpExecutor.builder().build();

      // test get
      server.setRes(200, "{'responseValue':'testValue'}");
      Assertions.assertNull(
          client
              .delete("http://localhost:" + server.port() + "/test")
              .toCompletableFuture()
              .join()
              .body());

      // check request
      Assertions.assertNotNull(server.req);
      Assertions.assertEquals("delete", server.req.method.toLowerCase());
      Assertions.assertEquals("/test", server.req.path);
      Assertions.assertNull(server.req.body);
      Assertions.assertNull(server.req.query);

      //       add not found exception
      server.setRes(404, null);
      var exception =
          Assertions.assertInstanceOf(
              HttpRequestException.class,
              Assertions.assertThrows(
                      CompletionException.class,
                      () ->
                          client
                              .delete("http://localhost:" + server.port() + "/test")
                              .toCompletableFuture()
                              .join())
                  .getCause());

      Assertions.assertEquals("get error code: 404", exception.getMessage());
      Assertions.assertEquals(404, exception.code());
    }
  }

  @Test
  void testErrorMessage() {
    try (var server = new Server()) {
      var client =
          HttpExecutor.builder()
              .errorMessageHandler(
                  (b, c) ->
                      Objects.requireNonNull(c.fromJson(b, TypeRef.map(String.class)).get("error")))
              .build();

      // add error response
      server.setRes(500, "{\"xx\":\"aa\",\"error\":\"hello world\"}");

      var exception =
          Assertions.assertInstanceOf(
              HttpRequestException.class,
              Assertions.assertThrows(
                      CompletionException.class,
                      () ->
                          client
                              .get(
                                  "http://localhost:" + server.port() + "/aaa",
                                  TypeRef.map(String.class))
                              .toCompletableFuture()
                              .join())
                  .getCause());
      Assertions.assertEquals("hello world", exception.getMessage());
      Assertions.assertEquals(500, exception.code());
    }
  }

  public static class TestResponse {
    private String responseValue;
  }

  public static class TestRequest {
    private final String requestValue;

    public TestRequest(String requestValue) {
      this.requestValue = requestValue;
    }
  }

  private static class Req {
    private final String method;
    private final String path;
    private final String query;
    private final String body;

    private Req(String method, String path, String query, String body) {
      this.method = method;
      this.path = path;
      this.query = query;
      this.body = body;
    }
  }

  private static class Res {
    private final int code;
    private final String body;

    private Res(int code, String body) {
      this.code = code;
      this.body = body;
    }
  }

  private static class Server implements AutoCloseable {
    private final HttpServer server;

    private final BlockingQueue<Res> res = new ArrayBlockingQueue<>(1);

    private volatile Req req;

    private Server() {
      try {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.start();
        server.createContext(
            "/",
            exchange -> {
              var body = exchange.getRequestBody().readAllBytes();
              req =
                  new Req(
                      exchange.getRequestMethod(),
                      exchange.getRequestURI().getPath(),
                      exchange.getRequestURI().getQuery(),
                      body == null || body.length == 0
                          ? null
                          : new String(body, StandardCharsets.UTF_8));
              try {
                var r = res.take();
                exchange.sendResponseHeaders(r.code, r.body == null ? 0 : r.body.length());
                try (var output = exchange.getResponseBody()) {
                  if (r.body != null) output.write(r.body.getBytes(StandardCharsets.UTF_8));
                }
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private void setRes(int code, String body) {
      Assertions.assertTrue(res.add(new Res(code, body)));
    }

    private int port() {
      return server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(5);
    }
  }
}
