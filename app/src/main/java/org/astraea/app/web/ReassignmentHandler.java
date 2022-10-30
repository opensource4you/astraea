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

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.common.FutureUtils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;

public class ReassignmentHandler implements Handler {
  static final String PLANS_KEY = "plans";
  static final String TOPIC_KEY = "topic";
  static final String PARTITION_KEY = "partition";
  static final String FROM_KEY = "from";
  static final String TO_KEY = "to";
  static final String BROKER_KEY = "broker";
  static final String EXCLUDE_KEY = "exclude";
  private final Admin admin;

  ReassignmentHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    return FutureUtils.sequence(
            channel.request().requests(PLANS_KEY).stream()
                .map(
                    request -> {
                      // case 0: move replica to another folder
                      if (request.has(BROKER_KEY, TOPIC_KEY, PARTITION_KEY, TO_KEY))
                        return admin
                            .moveToFolders(
                                Map.of(
                                    TopicPartitionReplica.of(
                                        request.value(TOPIC_KEY),
                                        request.intValue(PARTITION_KEY),
                                        request.intValue(BROKER_KEY)),
                                    request.value(TO_KEY)))
                            .thenApply(ignored -> Response.ACCEPT)
                            .toCompletableFuture();

                      // case 1: move replica to another broker
                      if (request.has(TOPIC_KEY, PARTITION_KEY, TO_KEY))
                        return admin
                            .moveToBrokers(
                                Map.of(
                                    TopicPartition.of(
                                        request.value(TOPIC_KEY), request.intValue(PARTITION_KEY)),
                                    request.intValues(TO_KEY)))
                            .thenApply(ignored -> Response.ACCEPT)
                            .toCompletableFuture();

                      if (request.has(EXCLUDE_KEY))
                        return admin
                            .brokers()
                            .thenCompose(
                                brokers -> {
                                  if (brokers.size() <= 1)
                                    return CompletableFuture.completedFuture(Response.BAD_REQUEST);
                                  var excludedBroker =
                                      brokers.stream()
                                          .filter(b -> b.id() == request.intValue(EXCLUDE_KEY))
                                          .findFirst();
                                  if (excludedBroker.isEmpty())
                                    return CompletableFuture.completedFuture(Response.BAD_REQUEST);
                                  var availableBrokers =
                                      brokers.stream()
                                          .filter(b -> b.id() != request.intValue(EXCLUDE_KEY))
                                          .collect(Collectors.toList());
                                  var partitions =
                                      excludedBroker.get().topicPartitions().stream()
                                          .filter(
                                              tp ->
                                                  !request.has(TOPIC_KEY)
                                                      || tp.topic()
                                                          .equals(request.value(TOPIC_KEY)))
                                          .collect(Collectors.toSet());
                                  if (partitions.isEmpty())
                                    return CompletableFuture.completedFuture(Response.BAD_REQUEST);
                                  var req =
                                      partitions.stream()
                                          .collect(
                                              Collectors.toMap(
                                                  tp -> tp,
                                                  tp -> {
                                                    var ids =
                                                        availableBrokers.stream()
                                                            .filter(
                                                                b ->
                                                                    b.topicPartitions()
                                                                        .contains(tp))
                                                            .map(NodeInfo::id)
                                                            .collect(Collectors.toList());
                                                    if (!ids.isEmpty()) return ids;
                                                    return List.of(
                                                        availableBrokers
                                                            .get(
                                                                (int)
                                                                    (Math.random()
                                                                        * availableBrokers.size()))
                                                            .id());
                                                  }));
                                  return admin
                                      .moveToBrokers(req)
                                      .thenApply(ignored -> Response.ACCEPT);
                                })
                            .toCompletableFuture();
                      return CompletableFuture.completedFuture(Response.BAD_REQUEST);
                    })
                .collect(Collectors.toUnmodifiableList()))
        .thenApply(
            rs -> {
              if (!rs.isEmpty() && rs.stream().allMatch(r -> r == Response.ACCEPT))
                return Response.ACCEPT;
              else return Response.BAD_REQUEST;
            });
  }

  @Override
  public CompletionStage<Reassignments> get(Channel channel) {
    return admin
        .topicNames(true)
        .thenApply(
            names ->
                names.stream()
                    .filter(name -> channel.target().map(Set::of).orElse(names).contains(name))
                    .collect(Collectors.toSet()))
        .thenCompose(admin::addingReplicas)
        .thenApply(
            rs ->
                new Reassignments(
                    rs.stream().map(AddingReplica::new).collect(Collectors.toUnmodifiableList())));
  }

  static class AddingReplica implements Response {
    final String topicName;
    final int partition;

    final int broker;
    final String dataFolder;

    final long size;

    final long leaderSize;
    final String progress;

    AddingReplica(org.astraea.common.admin.AddingReplica addingReplica) {
      this.topicName = addingReplica.topic();
      this.partition = addingReplica.partition();
      this.broker = addingReplica.broker();
      this.dataFolder = addingReplica.path();
      this.size = addingReplica.size();
      this.leaderSize = addingReplica.leaderSize();
      this.progress = progressInPercentage(leaderSize == 0 ? 0 : size / leaderSize);
    }
  }

  static class Reassignments implements Response {
    final Collection<AddingReplica> addingReplicas;

    Reassignments(Collection<AddingReplica> addingReplicas) {
      this.addingReplicas = addingReplicas;
    }
  }

  // visible for testing
  static String progressInPercentage(double progress) {
    // min(max(progress, 0), 1) is to force valid progress value is 0 < progress < 1
    // in case something goes wrong (maybe compacting cause data size shrinking suddenly)
    return String.format("%.2f%%", min(max(progress, 0), 1) * 100);
  }
}
