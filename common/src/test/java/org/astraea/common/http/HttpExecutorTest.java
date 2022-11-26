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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.astraea.common.Utils;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.junit.jupiter.api.Test;

class HttpExecutorTest {
  private static final JsonConverter jsonConverter = JsonConverter.defaultConverter();

  @Test
  void testGet() {
    var httpExecutor = HttpExecutor.builder().build();
    HttpTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test",
                HttpTestUtil.createTextHandler(List.of("GET"), "{'responseValue':'testValue'}")),
        x -> {
          var responseHttpResponse =
              httpExecutor
                  .get(getUrl(x, "/test"), TypeRef.of(TestResponse.class))
                  .thenApply(Response::body)
                  .toCompletableFuture()
                  .join();
          assertEquals("testValue", responseHttpResponse.responseValue());

          var executionException =
              assertThrows(
                  CompletionException.class,
                  () ->
                      httpExecutor
                          .get(getUrl(x, "/NotFound"), TypeRef.of(TestResponse.class))
                          .thenApply(Response::body)
                          .toCompletableFuture()
                          .join());
          assertEquals(StringResponseException.class, executionException.getCause().getClass());
        });
  }

  @Test
  void testGetParameter() {
    var httpExecutor = HttpExecutor.builder().build();
    HttpTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test",
                HttpTestUtil.createTextHandler(
                    List.of("GET"),
                    x -> assertEquals("/test?k1=v1", x.uri().toString()),
                    "{'responseValue':'testValue'}")),
        x -> {
          var responseHttpResponse =
              httpExecutor
                  .get(getUrl(x, "/test"), Map.of("k1", "v1"), TypeRef.of(TestResponse.class))
                  .thenApply(Response::body)
                  .toCompletableFuture()
                  .join();
          assertEquals("testValue", responseHttpResponse.responseValue());

          var executionException =
              assertThrows(
                  CompletionException.class,
                  () ->
                      httpExecutor
                          .get(getUrl(x, "/NotFound"), TypeRef.of(TestResponse.class))
                          .thenApply(Response::body)
                          .toCompletableFuture()
                          .join());
          assertEquals(StringResponseException.class, executionException.getCause().getClass());
        });
  }

  @Test
  void testGetNestedObject() {
    var httpExecutor = HttpExecutor.builder().build();
    HttpTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test", HttpTestUtil.createTextHandler(List.of("GET"), "['v1','v2']")),
        x -> {
          var responseHttpResponse =
              httpExecutor
                  .get(getUrl(x, "/test"), new TypeRef<List<String>>() {})
                  .thenApply(Response::body)
                  .toCompletableFuture()
                  .join();
          assertEquals(List.of("v1", "v2"), responseHttpResponse);
        });
  }

  @Test
  void testPost() {
    var httpExecutor = HttpExecutor.builder().build();
    HttpTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test",
                HttpTestUtil.createTextHandler(
                    List.of("Post"),
                    (x) -> {
                      var request = jsonConverter.fromJson(x.body(), TypeRef.of(TestRequest.class));
                      assertEquals("testRequestValue", request.requestValue());
                    },
                    "{'responseValue':'testValue'}")),
        x -> {
          var request = new TestRequest();
          request.setRequestValue("testRequestValue");
          var responseHttpResponse =
              httpExecutor
                  .post(getUrl(x, "/test"), request, TypeRef.of(TestResponse.class))
                  .thenApply(Response::body)
                  .toCompletableFuture()
                  .join();
          assertEquals("testValue", responseHttpResponse.responseValue());

          // response body can't convert to testResponse
          var executionException =
              assertThrows(
                  CompletionException.class,
                  () ->
                      httpExecutor
                          .post(getUrl(x, "/NotFound"), request, TypeRef.of(TestResponse.class))
                          .thenApply(Response::body)
                          .toCompletableFuture()
                          .join());
          assertEquals(StringResponseException.class, executionException.getCause().getClass());
        });
  }

  @Test
  void testPut() {
    var httpExecutor = HttpExecutor.builder().build();
    HttpTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test",
                HttpTestUtil.createTextHandler(
                    List.of("Put"),
                    (x) -> {
                      var request = jsonConverter.fromJson(x.body(), TypeRef.of(TestRequest.class));
                      assertEquals("testRequestValue", request.requestValue());
                    },
                    "{'responseValue':'testValue'}")),
        x -> {
          var request = new TestRequest();
          request.setRequestValue("testRequestValue");
          var responseHttpResponse =
              httpExecutor
                  .put(getUrl(x, "/test"), request, TypeRef.of(TestResponse.class))
                  .thenApply(Response::body)
                  .toCompletableFuture()
                  .join();
          assertEquals("testValue", responseHttpResponse.responseValue());

          var executionException =
              assertThrows(
                  CompletionException.class,
                  () ->
                      httpExecutor
                          .put(getUrl(x, "/NotFound"), request, TypeRef.of(TestResponse.class))
                          .thenApply(Response::body)
                          .toCompletableFuture()
                          .join());
          assertEquals(StringResponseException.class, executionException.getCause().getClass());
        });
  }

  @Test
  void testDelete() {
    var httpExecutor = HttpExecutor.builder().build();
    HttpTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test", HttpTestUtil.createTextHandler(List.of("delete"), "")),
        x -> {
          httpExecutor.delete(getUrl(x, "/test"));

          var executionException =
              assertThrows(
                  CompletionException.class,
                  () ->
                      httpExecutor
                          .delete(getUrl(x, "/NotFound"))
                          .thenApply(Response::body)
                          .toCompletableFuture()
                          .join());
          assertEquals(StringResponseException.class, executionException.getCause().getClass());
        });
  }

  @Test
  void testException() {
    var httpExecutor = HttpExecutor.builder().build();
    HttpTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test", HttpTestUtil.createErrorHandler(405, "don't allow this method to you!!!")),
        x ->
            Utils.packException(
                () -> {
                  var respException =
                      assertThrows(
                          CompletionException.class,
                          () ->
                              httpExecutor
                                  .get(getUrl(x, "/test"), TypeRef.of(TestResponse.class))
                                  .thenApply(Response::body)
                                  .toCompletableFuture()
                                  .join());

                  var voidException =
                      assertThrows(
                          CompletionException.class,
                          () ->
                              httpExecutor.delete(getUrl(x, "/test")).toCompletableFuture().join());

                  List.of(respException, voidException)
                      .forEach(
                          e -> {
                            assertEquals(StringResponseException.class, e.getCause().getClass());
                            var stringResponseException = (StringResponseException) e.getCause();
                            assertEquals(405, stringResponseException.statusCode());
                            assertEquals(
                                "don't allow this method to you!!!",
                                stringResponseException.body());
                            assertTrue(
                                stringResponseException
                                    .toString()
                                    .contains("don't allow this method to you!!!"));
                          });
                }));
  }

  private String getUrl(InetSocketAddress socketAddress, String path) {
    return "http://localhost:" + socketAddress.getPort() + path;
  }

  public static class TestResponse {
    private String responseValue;

    public String responseValue() {
      return responseValue;
    }

    public void setResponseValue(String responseValue) {
      this.responseValue = responseValue;
    }
  }

  public static class TestRequest {
    private String requestValue;

    public String requestValue() {
      return requestValue;
    }

    public void setRequestValue(String requestValue) {
      this.requestValue = requestValue;
    }
  }
}
