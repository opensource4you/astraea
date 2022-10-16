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

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.TopicPartition;

class BrokerHandler implements Handler {

  private final AsyncAdmin admin;

  BrokerHandler(AsyncAdmin admin) {
    this.admin = admin;
  }

  CompletionStage<Set<Integer>> brokers(Optional<String> target) {

    try {
      return target
          .map(id -> CompletableFuture.completedStage(Set.of(Integer.parseInt(id))))
          .orElseGet(
              () ->
                  admin
                      .nodeInfos()
                      .thenApply(ns -> ns.stream().map(NodeInfo::id).collect(Collectors.toSet())));
    } catch (NumberFormatException e) {
      return CompletableFuture.failedFuture(
          new NoSuchElementException("the broker id must be number"));
    }
  }

  @Override
  public CompletionStage<Response> get(Channel channel) {
    return brokers(channel.target())
        .thenCompose(
            ids ->
                admin
                    .brokers()
                    .thenApply(
                        brokers ->
                            brokers.stream()
                                .filter(b -> ids.contains(b.id()))
                                .map(Broker::new)
                                .collect(Collectors.toList())))
        .thenApply(
            brokers -> {
              if (brokers.isEmpty()) throw new NoSuchElementException("no brokers are found");
              if (channel.target().isPresent() && brokers.size() == 1) return brokers.get(0);
              return new Brokers(brokers);
            });
  }

  static class Topic implements Response {
    final String topic;
    final int partitionCount;

    Topic(String topic, int partitionCount) {
      this.topic = topic;
      this.partitionCount = partitionCount;
    }
  }

  static class Broker implements Response {
    final int id;
    final List<Topic> topics;
    final Map<String, String> configs;

    Broker(org.astraea.common.admin.Broker broker) {
      this.id = broker.id();
      this.topics =
          broker.topicPartitions().stream()
              .collect(Collectors.groupingBy(TopicPartition::topic))
              .entrySet()
              .stream()
              .map(e -> new Topic(e.getKey(), e.getValue().size()))
              .collect(Collectors.toUnmodifiableList());
      this.configs = broker.config().raw();
    }
  }

  static class Brokers implements Response {
    final List<Broker> brokers;

    Brokers(List<Broker> brokers) {
      this.brokers = brokers;
    }
  }
}
