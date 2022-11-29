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
package org.astraea.connector;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.astraea.common.http.HttpExecutor;
import org.astraea.common.http.Response;
import org.astraea.common.json.TypeRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ConnectorClientBuilderTest {

  @Test
  void testUrlShouldSet() throws MalformedURLException {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ConnectorClient.builder().build());

    ConnectorClient.builder().url(new URL("https://github.com/skiptests/astraea/")).build();
    ConnectorClient.builder()
        .urls(Set.of(new URL("https://github.com/skiptests/astraea/")))
        .build();
  }

  @Test
  void testExecutor() throws MalformedURLException, ExecutionException, InterruptedException {
    var httpExecutor = Mockito.mock(HttpExecutor.class);
    Mockito.when(httpExecutor.get(Mockito.any(), Mockito.eq(TypeRef.set(String.class))))
        .thenReturn(
            CompletableFuture.completedFuture(
                new Response<>() {
                  @Override
                  public int statusCode() {
                    return 200;
                  }

                  @Override
                  public Set<String> body() {
                    return Set.of("SpecialConnectorName");
                  }
                }));

    var connectorClient =
        ConnectorClient.builder()
            .url(new URL("http://localhost"))
            .httpExecutor(httpExecutor)
            .build();
    var connectorNames = connectorClient.connectorNames().toCompletableFuture().get();
    Assertions.assertEquals(1, connectorNames.size());
    Assertions.assertTrue(connectorNames.contains("SpecialConnectorName"));
  }
}
