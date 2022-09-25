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
package org.astraea.common.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.gson.Gson;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import org.junit.jupiter.api.Test;

class HttpExecutorTest {
  private static final Gson gson = new Gson();

  @Test
  void testGet() {
    var httpExecutor = HttpExecutor.builder().build();
    ConnectorTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test",
                ConnectorTestUtil.createTextHandler(
                    List.of("GET"), "{'responseValue':'testValue'}")),
        x -> {
          var responseHttpResponse = httpExecutor.get(getUrl(x, "/test"), TestResponse.class);
          assertEquals("testValue", responseHttpResponse.body().responseValue());

          assertThrows(
              StringResponseException.class,
              () -> httpExecutor.get(getUrl(x, "/NotFound"), TestResponse.class));
        });
  }

  @Test
  void testPost() {
    var httpExecutor = HttpExecutor.builder().build();
    ConnectorTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test",
                ConnectorTestUtil.createTextHandler(
                    List.of("Post"),
                    (x) -> {
                      var request = gson.fromJson(x, TestRequest.class);
                      assertEquals("testRequestValue", request.requestValue());
                    },
                    "{'responseValue':'testValue'}")),
        x -> {
          var request = new TestRequest();
          request.setRequestValue("testRequestValue");
          var responseHttpResponse =
              httpExecutor.post(getUrl(x, "/test"), request, TestResponse.class);
          assertEquals("testValue", responseHttpResponse.body().responseValue());

          // response body can't convert to testResponse
          assertThrows(
              StringResponseException.class,
              () -> httpExecutor.post(getUrl(x, "/NotFound"), request, TestResponse.class));
        });
  }

  @Test
  void testPut() {
    var httpExecutor = HttpExecutor.builder().build();
    ConnectorTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test",
                ConnectorTestUtil.createTextHandler(
                    List.of("Put"),
                    (x) -> {
                      var request = gson.fromJson(x, TestRequest.class);
                      assertEquals("testRequestValue", request.requestValue());
                    },
                    "{'responseValue':'testValue'}")),
        x -> {
          var request = new TestRequest();
          request.setRequestValue("testRequestValue");
          var responseHttpResponse =
              httpExecutor.put(getUrl(x, "/test"), request, TestResponse.class);
          assertEquals("testValue", responseHttpResponse.body().responseValue());

          assertThrows(
              StringResponseException.class,
              () -> httpExecutor.put(getUrl(x, "/NotFound"), request, TestResponse.class));
        });
  }

  @Test
  void testDelete() {
    var httpExecutor = HttpExecutor.builder().build();
    ConnectorTestUtil.testWithServer(
        httpServer ->
            httpServer.createContext(
                "/test", ConnectorTestUtil.createTextHandler(List.of("delete"), "")),
        x -> {
          httpExecutor.delete(getUrl(x, "/test"));

          assertThrows(
              StringResponseException.class, () -> httpExecutor.delete(getUrl(x, "/NotFound")));
        });
  }

  private URL getUrl(InetSocketAddress socketAddress, String path) {
    try {
      return new URL("http://localhost:" + socketAddress.getPort() + path);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
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
