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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.app.web.Request.RequestObject;
import org.astraea.common.FutureUtils;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;

public class ReassignmentHandler implements Handler {
  private final Admin admin;

  ReassignmentHandler(Admin admin) {
    this.admin = admin;
  }

  static class ReassignmentPostRequest implements Request {
    private List<Plan> plans = List.of();

    public ReassignmentPostRequest() {}

    public List<Plan> plans() {
      return plans;
    }
  }

  static class Plan implements RequestObject {
    private Optional<Integer> broker = Optional.empty();
    private Optional<String> topic = Optional.empty();
    private Optional<Integer> partition = Optional.empty();

    /** `to` has two different meanings */
    private Optional<Object> to = Optional.empty();

    private Optional<Integer> exclude = Optional.empty();

    public Plan() {}

    public Optional<Integer> broker() {
      return broker;
    }

    public Optional<String> topic() {
      return topic;
    }

    public Optional<Integer> partition() {
      return partition;
    }

    public Optional<Object> to() {
      return to;
    }

    public Optional<Integer> exclude() {
      return exclude;
    }
  }

  @Override
  public CompletionStage<Response> post(Channel channel) {
    var request = channel.request(TypeRef.of(ReassignmentPostRequest.class));
    return FutureUtils.sequence(
            request.plans().stream()
                .map(
                    plan -> {
                      // case 0: move replica to another folder
                      if (Utils.isPresent(plan.broker(), plan.topic(), plan.partition(), plan.to()))
                        return admin
                            .moveToFolders(
                                Map.of(
                                    TopicPartitionReplica.of(
                                        plan.topic().get(),
                                        plan.partition().get(),
                                        plan.broker().get()),
                                    JsonConverter.defaultConverter()
                                        .convert(plan.to().get(), TypeRef.of(String.class))))
                            .thenApply(ignored -> Response.ACCEPT)
                            .toCompletableFuture();

                      // case 1: move replica to another broker
                      if (Utils.isPresent(plan.topic(), plan.partition(), plan.to()))
                        return admin
                            .moveToBrokers(
                                Map.of(
                                    TopicPartition.of(plan.topic().get(), plan.partition().get()),
                                    JsonConverter.defaultConverter()
                                        .convert(plan.to().get(), TypeRef.array(Integer.class))))
                            .thenApply(ignored -> Response.ACCEPT)
                            .toCompletableFuture();

                      if (plan.exclude().isPresent())
                        return admin
                            .brokers()
                            .thenCompose(
                                brokers -> {
                                  var exclude = plan.exclude().get();
                                  var excludedBroker =
                                      brokers.stream().filter(b -> b.id() == exclude).findFirst();
                                  if (excludedBroker.isEmpty())
                                    return CompletableFuture.completedFuture(Response.BAD_REQUEST);
                                  var availableBrokers =
                                      brokers.stream()
                                          .filter(b -> b.id() != exclude)
                                          .collect(Collectors.toList());
                                  var partitions =
                                      excludedBroker.get().topicPartitions().stream()
                                          .filter(
                                              tp ->
                                                  plan.topic().isEmpty()
                                                      || tp.topic().equals(plan.topic().get()))
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
        .thenCompose(admin::clusterInfo)
        .thenApply(
            clusterInfo -> {
              var leaderSizes = ClusterInfo.leaderSize(clusterInfo);
              return new Reassignments(
                  clusterInfo
                      .replicaStream()
                      .filter(ReplicaInfo::isAdding)
                      .map(
                          r ->
                              new AddingReplica(
                                  r, leaderSizes.getOrDefault(r.topicPartition(), 0L)))
                      .collect(Collectors.toUnmodifiableList()));
            });
  }

  static class AddingReplica implements Response {
    final String topicName;
    final int partition;

    final int broker;
    final String dataFolder;

    final long size;

    final long leaderSize;
    final String progress;

    AddingReplica(Replica addingReplica, long leaderSize) {
      this.topicName = addingReplica.topic();
      this.partition = addingReplica.partition();
      this.broker = addingReplica.nodeInfo().id();
      this.dataFolder = addingReplica.path();
      this.size = addingReplica.size();
      this.leaderSize = leaderSize;
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
