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

import com.fasterxml.jackson.annotation.JsonAlias;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.DataRate;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.QuotaConfigs;
import org.astraea.common.json.TypeRef;

public class QuotaHandler implements Handler {

  private final Admin admin;

  QuotaHandler(Admin admin) {
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

  static class QuotaPostRequest implements Request {
    private Optional<String> ip = Optional.empty();

    @JsonAlias("connection_creation_rate")
    private Optional<Integer> connectionCreationRate = Optional.empty();

    @JsonAlias("client-id")
    private Optional<String> clientId = Optional.empty();

    @JsonAlias("producer_byte_rate")
    private Optional<Long> producerByteRate = Optional.empty();

    @JsonAlias("consumer_byte_rate")
    private Optional<Long> consumerByteRate = Optional.empty();

    public QuotaPostRequest() {}

    public Optional<String> ip() {
      return ip;
    }

    public Optional<Integer> connectionCreationRate() {
      return connectionCreationRate;
    }

    public Optional<String> clientId() {
      return clientId;
    }

    public Optional<Long> producerByteRate() {
      return producerByteRate;
    }

    public Optional<Long> consumerByteRate() {
      return consumerByteRate;
    }
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    var postRequest = channel.request().getRequest(TypeRef.of(QuotaPostRequest.class));

    if (isPresent(postRequest.ip(), postRequest.connectionCreationRate()))
      return admin
          .setConnectionQuotas(
              Map.of(postRequest.ip().get(), postRequest.connectionCreationRate().get()))
          .thenCompose(
              ignored ->
                  admin
                      .quotas(Map.of(QuotaConfigs.IP, Set.of(postRequest.ip().get())))
                      .thenApply(Quotas::new));

    // TODO: use DataRate#Field (traced https://github.com/skiptests/astraea/issues/488)
    // see https://github.com/skiptests/astraea/issues/490
    if (postRequest.clientId().isPresent()) {
      var clientId = postRequest.clientId().get();

      List<Supplier<CompletionStage<Void>>> quotaStages = new ArrayList<>();
      if (postRequest.producerByteRate().isPresent()) {
        quotaStages.add(
            () ->
                admin.setProducerQuotas(
                    Map.of(
                        clientId,
                        DataRate.Byte.of(postRequest.producerByteRate().get()).perSecond())));
      }
      if (postRequest.consumerByteRate().isPresent()) {
        quotaStages.add(
            () ->
                admin.setConsumerQuotas(
                    Map.of(
                        clientId,
                        DataRate.Byte.of(postRequest.consumerByteRate().get()).perSecond())));
      }

      if (!quotaStages.isEmpty()) {
        return runSequence(quotaStages)
            .thenCompose(
                ignored ->
                    admin
                        .quotas(Map.of(QuotaConfigs.CLIENT_ID, Set.of(clientId)))
                        .thenApply(Quotas::new));
      }
    }

    return CompletableFuture.completedFuture(Response.NOT_FOUND);
  }

  private CompletionStage<Void> runSequence(
      List<Supplier<CompletionStage<Void>>> completeSuppliers) {
    if (completeSuppliers.size() == 0) {
      return CompletableFuture.completedStage(null);
    }
    if (completeSuppliers.size() == 1) {
      return completeSuppliers.get(0).get();
    }
    var current = completeSuppliers.get(0).get();
    for (Supplier<CompletionStage<Void>> completeSupplier :
        completeSuppliers.subList(1, completeSuppliers.size())) {
      current = current.thenCompose(ignored -> completeSupplier.get());
    }
    return current;
  }

  private boolean isPresent(Optional<?>... fields) {
    return Arrays.stream(fields).allMatch(Optional::isPresent);
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
