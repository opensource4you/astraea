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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.common.DataRate;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.QuotaConfigs.QuotaKeys;
import org.astraea.common.json.TypeRef;

public class QuotaHandler implements Handler {

  private final Admin admin;

  QuotaHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public CompletionStage<Quotas> get(Channel channel) {
    if (channel.queries().containsKey(QuotaKeys.IP.value()))
      return admin
          .quotas(
              Map.of(
                  QuotaKeys.IP.kafkaValue(), Set.of(channel.queries().get(QuotaKeys.IP.value()))))
          .thenApply(Quotas::new);
    if (channel.queries().containsKey(QuotaKeys.CLIENT_ID.value()))
      return admin
          .quotas(
              Map.of(
                  QuotaKeys.CLIENT_ID.kafkaValue(),
                  Set.of(channel.queries().get(QuotaKeys.CLIENT_ID.value()))))
          .thenApply(Quotas::new);
    return admin.quotas().thenApply(Quotas::new);
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    var postRequest = channel.request(TypeRef.of(QuotaPostRequest.class));

    if (postRequest.connection.isPresent()) {
      var connectionQuota = postRequest.connection.get();
      return admin
          .setConnectionQuotas(Map.of(connectionQuota.ip, connectionQuota.connectionCreationRate))
          .thenCompose(
              ignored ->
                  admin
                      .quotas(Map.of(QuotaKeys.IP.kafkaValue(), Set.of(connectionQuota.ip)))
                      .thenApply(Quotas::new));
    }

    // TODO: use DataRate#Field (traced https://github.com/skiptests/astraea/issues/488)
    // see https://github.com/skiptests/astraea/issues/490
    if (postRequest.producer.isPresent()) {
      var producerQuota = postRequest.producer.get();
      return admin
          .setProducerQuotas(
              Map.of(
                  producerQuota.clientId,
                  DataRate.Byte.of(producerQuota.producerByteRate).perSecond()))
          .thenCompose(
              ignored ->
                  admin
                      .quotas(
                          Map.of(QuotaKeys.CLIENT_ID.kafkaValue(), Set.of(producerQuota.clientId)))
                      .thenApply(Quotas::new));
    }

    if (postRequest.consumer.isPresent()) {
      var consumerQuota = postRequest.consumer.get();
      return admin
          .setConsumerQuotas(
              Map.of(
                  consumerQuota.clientId,
                  DataRate.Byte.of(consumerQuota.consumerByteRate).perSecond()))
          .thenCompose(
              ignored ->
                  admin
                      .quotas(
                          Map.of(QuotaKeys.CLIENT_ID.kafkaValue(), Set.of(consumerQuota.clientId)))
                      .thenApply(Quotas::new));
    }

    return CompletableFuture.completedFuture(Response.NOT_FOUND);
  }

  static class QuotaPostRequest implements Request {
    private Optional<ConnectionQuota> connection = Optional.empty();
    private Optional<ProducerQuota> producer = Optional.empty();
    private Optional<ConsumerQuota> consumer = Optional.empty();

    public QuotaPostRequest() {}
  }

  static class ConnectionQuota {
    private String ip;
    private Integer connectionCreationRate;

    public ConnectionQuota() {}
  }

  static class ProducerQuota {
    private String clientId;
    private Long producerByteRate;

    public ProducerQuota() {}
  }

  static class ConsumerQuota {
    private String clientId;
    private Long consumerByteRate;

    public ConsumerQuota() {}
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
      this.target = new Target(convertValuefromKafka(target), targetValue);
      this.limit = new Limit(convertValuefromKafka(limit), limitValue);
    }

    private static String convertValuefromKafka(String value) {
      return Optional.ofNullable(QuotaKeys.fromKafka(value)).map(QuotaKeys::value).orElse(value);
    }
  }

  static class Quotas implements Response {
    final List<Quota> quotas;

    Quotas(Collection<org.astraea.common.admin.Quota> quotas) {
      this.quotas = quotas.stream().map(Quota::new).collect(Collectors.toUnmodifiableList());
    }
  }
}
