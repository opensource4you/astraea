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
import java.util.stream.Collectors;
import org.astraea.common.DataRate;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.QuotaConfigs;

public class QuotaHandler implements Handler {

  static final String IP_KEY = QuotaConfigs.IP;
  static final String CLIENT_ID_KEY = QuotaConfigs.CLIENT_ID;
  static final String CONNECTION_RATE_KEY = QuotaConfigs.IP_CONNECTION_RATE_CONFIG;
  static final String PRODUCE_RATE_KEY = QuotaConfigs.PRODUCER_BYTE_RATE_CONFIG;
  static final String CONSUME_RATE_KEY = QuotaConfigs.CONSUMER_BYTE_RATE_CONFIG;

  private final Admin admin;

  QuotaHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public Quotas get(Channel channel) {
    if (channel.queries().containsKey(IP_KEY))
      return new Quotas(
          admin.quotas(org.astraea.common.admin.Quota.Target.IP, channel.queries().get(IP_KEY)));
    if (channel.queries().containsKey(CLIENT_ID_KEY))
      return new Quotas(
          admin.quotas(
              org.astraea.common.admin.Quota.Target.CLIENT_ID,
              channel.queries().get(CLIENT_ID_KEY)));
    return new Quotas(admin.quotas());
  }

  @Override
  public Response post(Channel channel) {
    if (channel.request().get(IP_KEY).isPresent()) {
      admin
          .quotaCreator()
          .ip(channel.request().value(IP_KEY))
          .connectionRate(channel.request().getInt(CONNECTION_RATE_KEY).orElse(Integer.MAX_VALUE))
          .create();
      return new Quotas(
          admin.quotas(org.astraea.common.admin.Quota.Target.IP, channel.request().value(IP_KEY)));
    }
    if (channel.request().get(CLIENT_ID_KEY).isPresent()) {
      admin
          .quotaCreator()
          .clientId(channel.request().value(CLIENT_ID_KEY))
          // TODO: use DataRate#Field (traced https://github.com/skiptests/astraea/issues/488)
          // see https://github.com/skiptests/astraea/issues/490
          .produceRate(
              channel
                  .request()
                  .get(PRODUCE_RATE_KEY)
                  .map(Long::parseLong)
                  .map(v -> DataRate.Byte.of(v).perSecond())
                  .orElse(null))
          .consumeRate(
              channel
                  .request()
                  .get(CONSUME_RATE_KEY)
                  .map(Long::parseLong)
                  .map(v -> DataRate.Byte.of(v).perSecond())
                  .orElse(null))
          .create();
      return new Quotas(
          admin.quotas(
              org.astraea.common.admin.Quota.Target.CLIENT_ID,
              channel.request().value(CLIENT_ID_KEY)));
    }
    return Response.NOT_FOUND;
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
