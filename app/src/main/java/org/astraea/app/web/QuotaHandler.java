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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.common.DataRate;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.Quota;
import org.astraea.common.admin.QuotaConfigs;

public class QuotaHandler implements Handler {

  private final AsyncAdmin admin;

  QuotaHandler(AsyncAdmin admin) {
    this.admin = admin;
  }

  @Override
  public CompletionStage<Quotas> get(Channel channel) {
    if (channel.queries().containsKey(QuotaConfigs.IP))
      return admin
          .quotas(Map.of(QuotaConfigs.IP, Set.of(channel.queries().get(QuotaConfigs.IP))))
          .thenApply(Quotas::new);
    if (channel.queries().containsKey(QuotaConfigs.CLIENT_ID))
      return admin
          .quotas(
              Map.of(QuotaConfigs.CLIENT_ID, Set.of(channel.queries().get(QuotaConfigs.CLIENT_ID))))
          .thenApply(Quotas::new);
    return admin.quotas().thenApply(Quotas::new);
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    if (channel.request().has(QuotaConfigs.IP, QuotaConfigs.IP_CONNECTION_RATE_CONFIG))
      return admin
          .setConnectionQuotas(
              Map.of(
                  channel.request().value(QuotaConfigs.IP),
                  channel.request().intValue(QuotaConfigs.IP_CONNECTION_RATE_CONFIG)))
          .thenCompose(
              ignored ->
                  admin
                      .quotas(
                          Map.of(QuotaConfigs.IP, Set.of(channel.request().value(QuotaConfigs.IP))))
                      .thenApply(Quotas::new));

    // TODO: use DataRate#Field (traced https://github.com/skiptests/astraea/issues/488)
    // see https://github.com/skiptests/astraea/issues/490
    if (channel
        .request()
        .has(
            QuotaConfigs.CLIENT_ID,
            QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG,
            QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG))
      return admin
          .setProducerQuotas(
              Map.of(
                  channel.request().value(QuotaConfigs.CLIENT_ID),
                  DataRate.Byte.of(
                          channel.request().longValue(QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG))
                      .perSecond()))
          .thenCompose(
              ignored ->
                  admin.setConsumerQuotas(
                      Map.of(
                          channel.request().value(QuotaConfigs.CLIENT_ID),
                          DataRate.Byte.of(
                                  channel
                                      .request()
                                      .longValue(QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG))
                              .perSecond())))
          .thenCompose(
              ignored ->
                  admin
                      .quotas(
                          Map.of(
                              QuotaConfigs.CLIENT_ID,
                              Set.of(channel.request().value(QuotaConfigs.CLIENT_ID))))
                      .thenApply(Quotas::new));

    if (channel.request().has(QuotaConfigs.CLIENT_ID, QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG))
      return admin
          .setConsumerQuotas(
              Map.of(
                  channel.request().value(QuotaConfigs.CLIENT_ID),
                  DataRate.Byte.of(
                          channel.request().longValue(QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG))
                      .perSecond()))
          .thenCompose(
              ignored ->
                  admin
                      .quotas(
                          Map.of(
                              QuotaConfigs.CLIENT_ID,
                              Set.of(channel.request().value(QuotaConfigs.CLIENT_ID))))
                      .thenApply(Quotas::new));

    if (channel.request().has(QuotaConfigs.CLIENT_ID, QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG))
      return admin
          .setProducerQuotas(
              Map.of(
                  channel.request().value(QuotaConfigs.CLIENT_ID),
                  DataRate.Byte.of(
                          channel.request().longValue(QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG))
                      .perSecond()))
          .thenCompose(
              ignored ->
                  admin
                      .quotas(
                          Map.of(
                              QuotaConfigs.CLIENT_ID,
                              Set.of(channel.request().value(QuotaConfigs.CLIENT_ID))))
                      .thenApply(Quotas::new));

    return CompletableFuture.completedFuture(Response.NOT_FOUND);
  }

  static class Target implements Response {
    final String name;
    final String value;

    Target(String name, String value) {
      this.name = name;
      this.value = value;
    }
  }

  static class Limit implements Response {
    final String name;
    final double value;

    Limit(String name, double value) {
      this.name = name;
      this.value = value;
    }
  }

  static class Quota implements Response {
    final Target target;
    final Limit limit;

    public Quota(org.astraea.common.admin.Quota quota) {
      this(quota.targetKey(), quota.targetValue(), quota.limitKey(), quota.limitValue());
    }

    public Quota(String target, String targetValue, String limit, double limitValue) {
      this.target = new Target(target, targetValue);
      this.limit = new Limit(limit, limitValue);
    }
  }

  static class Quotas implements Response {
    final List<Quota> quotas;

    Quotas(Collection<org.astraea.common.admin.Quota> quotas) {
      this.quotas = quotas.stream().map(Quota::new).collect(Collectors.toUnmodifiableList());
    }
  }
}
