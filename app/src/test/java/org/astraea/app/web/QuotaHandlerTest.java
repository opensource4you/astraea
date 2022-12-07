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
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.QuotaConfigs.QuotaKeys;
import org.astraea.common.json.JsonConverter;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QuotaHandlerTest extends RequireBrokerCluster {
  private static final String CONNECTION = "connection";
  private static final String PRODUCER = "producer";
  private static final String CREATION_RATE = "connectionCreationRate";

  @Test
  void testCreateIPQuota() {
    var ip = "192.168.10.11";
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new QuotaHandler(admin);
      var result =
          Assertions.assertInstanceOf(
              QuotaHandler.Quotas.class,
              handler
                  .post(
                      Channel.ofRequest(
                          JsonConverter.defaultConverter()
                              .toJson(
                                  Map.of(
                                      CONNECTION,
                                      Map.of(QuotaKeys.IP.value(), ip, CREATION_RATE, "10")))))
                  .toCompletableFuture()
                  .join());
      Assertions.assertEquals(1, result.quotas.size());
      Assertions.assertEquals(QuotaKeys.IP.value(), result.quotas.iterator().next().target.name);
      Assertions.assertEquals(ip, result.quotas.iterator().next().target.value);
      Assertions.assertEquals(
          QuotaKeys.IP_CONNECTION_RATE.value(), result.quotas.iterator().next().limit.name);
      Assertions.assertEquals(10, result.quotas.iterator().next().limit.value);
    }
  }

  @Test
  void testCreateProducerQuota() {
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new QuotaHandler(admin);
      var result =
          Assertions.assertInstanceOf(
              QuotaHandler.Quotas.class,
              handler
                  .post(
                      Channel.ofRequest(
                          JsonConverter.defaultConverter()
                              .toJson(
                                  Map.of(
                                      PRODUCER,
                                      Map.of(
                                          QuotaKeys.CLIENT_ID.value(),
                                          "myClient",
                                          QuotaKeys.PRODUCER_BYTE_RATE.value(),
                                          "10")))))
                  .toCompletableFuture()
                  .join());
      Assertions.assertEquals(1, result.quotas.size());
      Assertions.assertEquals(
          QuotaKeys.CLIENT_ID.value(), result.quotas.iterator().next().target.name);
      Assertions.assertEquals("myClient", result.quotas.iterator().next().target.value);
      Assertions.assertEquals(
          QuotaKeys.PRODUCER_BYTE_RATE.value(), result.quotas.iterator().next().limit.name);
      Assertions.assertEquals(10, result.quotas.iterator().next().limit.value);
    }
  }

  @Test
  void testQuery() {
    var ip0 = "192.168.10.11";
    var ip1 = "192.168.10.12";
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new QuotaHandler(admin);

      handler
          .post(
              Channel.ofRequest(
                  JsonConverter.defaultConverter()
                      .toJson(
                          Map.of(
                              CONNECTION, Map.of(QuotaKeys.IP.value(), ip0, CREATION_RATE, "10")))))
          .toCompletableFuture()
          .join();
      handler
          .post(
              Channel.ofRequest(
                  JsonConverter.defaultConverter()
                      .toJson(
                          Map.of(
                              CONNECTION, Map.of(QuotaKeys.IP.value(), ip1, CREATION_RATE, "20")))))
          .toCompletableFuture()
          .join();
      Assertions.assertEquals(
          1,
          handler
              .get(Channel.ofQueries(Map.of(QuotaKeys.IP.value(), ip0)))
              .toCompletableFuture()
              .join()
              .quotas
              .size());
      Assertions.assertEquals(
          1,
          handler
              .get(Channel.ofQueries(Map.of(QuotaKeys.IP.value(), ip1)))
              .toCompletableFuture()
              .join()
              .quotas
              .size());
    }
  }

  @Test
  void testQueryNonexistentQuota() {
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new QuotaHandler(admin);
      Assertions.assertEquals(
          0,
          Assertions.assertInstanceOf(
                  QuotaHandler.Quotas.class,
                  handler
                      .get(Channel.ofQueries(Map.of(QuotaKeys.IP.value(), "unknown")))
                      .toCompletableFuture()
                      .join())
              .quotas
              .size());

      Assertions.assertEquals(
          0,
          Assertions.assertInstanceOf(
                  QuotaHandler.Quotas.class,
                  handler
                      .get(Channel.ofQueries(Map.of(QuotaKeys.CLIENT_ID.value(), "unknown")))
                      .toCompletableFuture()
                      .join())
              .quotas
              .size());
    }
  }
}
