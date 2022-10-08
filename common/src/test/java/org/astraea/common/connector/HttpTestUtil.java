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

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class HttpTestUtil {

  public static void testWithServer(
      Consumer<HttpServer> serverSettingsConsumer,
      Consumer<InetSocketAddress> inetSocketAddressConsumer) {
    HttpServer server = null;
    {
      try {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        serverSettingsConsumer.accept(server);
        server.setExecutor(null);
        server.start();

        inetSocketAddressConsumer.accept(server.getAddress());
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        if (Objects.nonNull(server)) {
          server.stop(5);
        }
      }
    }
  }

  public static HttpHandler createTextHandler(List<String> requestMethod, String responseText) {
    return createTextHandler(requestMethod, (x) -> {}, responseText);
  }

  public static HttpHandler createTextHandler(
      List<String> requestMethod, Consumer<RequestInfo> requestAsserter, String responseText) {
    return exchange -> {
      if (requestMethod.stream().noneMatch(x -> exchange.getRequestMethod().equalsIgnoreCase(x))) {
        throw new RuntimeException("Request method is not supported.");
      }

      var requestBody =
          new BufferedReader(new InputStreamReader(exchange.getRequestBody()))
              .lines()
              .collect(Collectors.joining());
      requestAsserter.accept(new RequestInfo(exchange.getRequestURI(), requestBody));
      exchange.sendResponseHeaders(200, responseText.length());
      OutputStream os = exchange.getResponseBody();
      os.write(responseText.getBytes());
      os.close();
    };
  }

  public static class RequestInfo {
    private final URI uri;
    private final String body;

    public RequestInfo(URI uri, String body) {
      this.uri = uri;
      this.body = body;
    }

    public URI uri() {
      return uri;
    }

    public String body() {
      return body;
    }
  }
}
