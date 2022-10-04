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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.astraea.common.Utils;
import org.astraea.common.Utils.Getter;
import org.astraea.common.connector.WorkerResponseException.WorkerError;

public class Builder {

  private static final Gson gson = new Gson();

  public ConnectorClient build(URL url) {
    var httpExecutor = HttpExecutor.builder().build();

    return new ConnectorClient() {
      @Override
      public WorkerInfo info() {
        return convertErrorException(() -> httpExecutor.get(getURL("/"), WorkerInfo.class).body());
      }

      @Override
      public List<String> connectors() {
        return convertErrorException(
            () ->
                httpExecutor
                    .<List<String>>get(
                        getURL("/connectors"), new TypeToken<List<String>>() {}.getType())
                    .body());
      }

      @Override
      public ConnectorInfo connector(String name) {
        return convertErrorException(
            () -> httpExecutor.get(getURL("/connectors/" + name), ConnectorInfo.class).body());
      }

      @Override
      public ConnectorInfo createConnector(String name, Map<String, String> config) {
        var connectorReq = new ConnectorReq(name, config);
        return convertErrorException(
            () ->
                httpExecutor.post(getURL("/connectors"), connectorReq, ConnectorInfo.class).body());
      }

      @Override
      public ConnectorInfo updateConnector(String name, Map<String, String> config) {
        return convertErrorException(
            () ->
                httpExecutor
                    .put(getURL("/connectors/" + name + "/config"), config, ConnectorInfo.class)
                    .body());
      }

      @Override
      public void deleteConnector(String name) {
        convertErrorException(() -> httpExecutor.delete(getURL("/connectors/" + name)));
      }

      private URL getURL(String path) {
        try {
          return url.toURI().resolve(path).toURL();
        } catch (MalformedURLException | URISyntaxException e) {
          throw new RuntimeException(e);
        }
      }

      private <R> R convertErrorException(Getter<R> getter) {
        return Utils.packException(
            () -> {
              try {
                return getter.get();
              } catch (StringResponseException stringResponseException) {
                var workerError =
                    Objects.isNull(stringResponseException.errorMsg())
                        ? new WorkerError(
                            stringResponseException.httpResponse().statusCode(),
                            "Unspecified error")
                        : gson.fromJson(stringResponseException.errorMsg(), WorkerError.class);
                throw new WorkerResponseException(
                    String.format(
                        "Error code %s, %s", workerError.error_code(), workerError.message()),
                    stringResponseException,
                    workerError);
              }
            });
      }
    };
  }
}
