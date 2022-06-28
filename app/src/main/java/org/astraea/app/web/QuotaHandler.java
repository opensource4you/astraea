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
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;

public class QuotaHandler implements Handler {

  static final String IP_KEY = org.astraea.app.admin.Quota.Target.IP.nameOfKafka();
  static final String CLIENT_ID_KEY = org.astraea.app.admin.Quota.Target.CLIENT_ID.nameOfKafka();
  static final String CONNECTION_RATE_KEY =
      org.astraea.app.admin.Quota.Limit.IP_CONNECTION_RATE.nameOfKafka();
  static final String PRODUCE_RATE_KEY =
      org.astraea.app.admin.Quota.Limit.PRODUCER_BYTE_RATE.nameOfKafka();
  static final String CONSUME_RATE_KEY =
      org.astraea.app.admin.Quota.Limit.CONSUMER_BYTE_RATE.nameOfKafka();

  private final Admin admin;

  QuotaHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public Quotas get(Optional<String> target, Map<String, String> queries) {
    if (queries.containsKey(IP_KEY))
      return new Quotas(admin.quotas(org.astraea.app.admin.Quota.Target.IP, queries.get(IP_KEY)));
    if (queries.containsKey(CLIENT_ID_KEY))
      return new Quotas(
          admin.quotas(org.astraea.app.admin.Quota.Target.CLIENT_ID, queries.get(CLIENT_ID_KEY)));
    return new Quotas(admin.quotas());
  }

  @Override
  public Response post(PostRequest request) {
    if (request.get(IP_KEY).isPresent()) {
      admin
          .quotaCreator()
          .ip(request.value(IP_KEY))
          .connectionRate(request.intValue(CONNECTION_RATE_KEY, Integer.MAX_VALUE))
          .create();
      return new Quotas(admin.quotas(org.astraea.app.admin.Quota.Target.IP, request.value(IP_KEY)));
    }
    if (request.get(CLIENT_ID_KEY).isPresent()) {
      admin
          .quotaCreator()
          .clientId(request.value(CLIENT_ID_KEY))
          .produceRate(request.intValue(PRODUCE_RATE_KEY, Integer.MAX_VALUE))
          .consumeRate(request.intValue(CONSUME_RATE_KEY, Integer.MAX_VALUE))
          .create();
      return new Quotas(
          admin.quotas(org.astraea.app.admin.Quota.Target.CLIENT_ID, request.value(CLIENT_ID_KEY)));
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

    public Quota(org.astraea.app.admin.Quota quota) {
      this(
          quota.target().nameOfKafka(),
          quota.targetValue(),
          quota.limit().nameOfKafka(),
          quota.limitValue());
    }

    public Quota(String target, String targetValue, String limit, double limitValue) {
      this.target = new Target(target, targetValue);
      this.limit = new Limit(limit, limitValue);
    }
  }

  static class Quotas implements Response {
    final List<Quota> quotas;

    Quotas(Collection<org.astraea.app.admin.Quota> quotas) {
      this.quotas = quotas.stream().map(Quota::new).collect(Collectors.toUnmodifiableList());
    }
  }
}
