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

import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.QuotaConfigs;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QuotaHandlerTest extends RequireBrokerCluster {

  @Test
  void testCreateQuota() throws ExecutionException, InterruptedException {
    var ip = "192.168.10.11";
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var handler = new QuotaHandler(admin);

      var result =
          Assertions.assertInstanceOf(
              QuotaHandler.Quotas.class,
              handler
                  .post(
                      Channel.ofRequest(
                          PostRequest.of(
                              Map.of(
                                  QuotaConfigs.IP,
                                  ip,
                                  QuotaConfigs.IP_CONNECTION_RATE_CONFIG,
                                  "10"))))
                  .toCompletableFuture()
                  .get());
      Assertions.assertEquals(1, result.quotas.size());
      Assertions.assertEquals(QuotaConfigs.IP, result.quotas.iterator().next().target.name);
      Assertions.assertEquals(ip, result.quotas.iterator().next().target.value);
      Assertions.assertEquals(
          QuotaConfigs.IP_CONNECTION_RATE_CONFIG, result.quotas.iterator().next().limit.name);
      Assertions.assertEquals(10, result.quotas.iterator().next().limit.value);
    }
  }

  @Test
  void testQuery() throws ExecutionException, InterruptedException {
    var ip0 = "192.168.10.11";
    var ip1 = "192.168.10.12";
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var handler = new QuotaHandler(admin);

      handler
          .post(
              Channel.ofRequest(
                  PostRequest.of(
                      Map.of(QuotaConfigs.IP, ip0, QuotaConfigs.IP_CONNECTION_RATE_CONFIG, "10"))))
          .toCompletableFuture()
          .get();
      handler
          .post(
              Channel.ofRequest(
                  PostRequest.of(
                      Map.of(QuotaConfigs.IP, ip1, QuotaConfigs.IP_CONNECTION_RATE_CONFIG, "20"))))
          .toCompletableFuture()
          .get();
      Assertions.assertEquals(
          1,
          handler
              .get(Channel.ofQueries(Map.of(QuotaConfigs.IP, ip0)))
              .toCompletableFuture()
              .get()
              .quotas
              .size());
      Assertions.assertEquals(
          1,
          handler
              .get(Channel.ofQueries(Map.of(QuotaConfigs.IP, ip1)))
              .toCompletableFuture()
              .get()
              .quotas
              .size());
    }
  }

  @Test
  void testQueryNonexistentQuota() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var handler = new QuotaHandler(admin);
      Assertions.assertEquals(
          0,
          Assertions.assertInstanceOf(
                  QuotaHandler.Quotas.class,
                  handler
                      .get(Channel.ofQueries(Map.of(QuotaConfigs.IP, "unknown")))
                      .toCompletableFuture()
                      .get())
              .quotas
              .size());

      Assertions.assertEquals(
          0,
          Assertions.assertInstanceOf(
                  QuotaHandler.Quotas.class,
                  handler
                      .get(Channel.ofQueries(Map.of(QuotaConfigs.CLIENT_ID, "unknown")))
                      .toCompletableFuture()
                      .get())
              .quotas
              .size());
    }
  }
}
