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
package org.astraea.common.scenario;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.util.Pair;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.admin.TopicPartitionReplica;

public class SkewedPartitionScenario implements Scenario {

  final String topicName;
  final int partitions;
  final short replicas;
  final double binomialProbability;

  public SkewedPartitionScenario(
      String topicName, int partitions, short replicas, double binomialProbability) {
    this.topicName = topicName;
    this.partitions = partitions;
    this.replicas = replicas;
    this.binomialProbability = binomialProbability;
  }

  @Override
  public CompletionStage<Result> apply(Admin admin) {
    return admin
        .creator()
        .topic(topicName)
        .numberOfPartitions(partitions)
        .numberOfReplicas(replicas)
        .run()
        .thenCompose(
            ignored ->
                admin.waitPartitionLeaderSynced(
                    Map.of(topicName, partitions), Duration.ofSeconds(4)))
        .thenCompose(ignored -> admin.brokers())
        .thenApply(
            brokers -> brokers.stream().map(NodeInfo::id).sorted().collect(Collectors.toList()))
        .thenCompose(
            brokerIds -> {
              var distribution =
                  new BinomialDistribution(brokerIds.size() - 1, binomialProbability);
              var replicaLists =
                  IntStream.range(0, partitions)
                      .boxed()
                      .collect(
                          Collectors.toUnmodifiableMap(
                              p -> TopicPartition.of(topicName, p),
                              ignore -> sampledReplicaList(brokerIds, replicas, distribution)));
              return admin
                  .moveToBrokers(replicaLists)
                  .thenCompose(
                      ignored ->
                          admin.waitReplicasSynced(
                              replicaLists.entrySet().stream()
                                  .flatMap(
                                      e ->
                                          e.getValue().stream()
                                              .map(
                                                  id ->
                                                      TopicPartitionReplica.of(
                                                          e.getKey().topic(),
                                                          e.getKey().partition(),
                                                          id)))
                                  .collect(Collectors.toSet()),
                              Duration.ofSeconds(30)))
                  .thenCompose(ignored -> admin.preferredLeaderElection(replicaLists.keySet()))
                  .thenCompose(
                      ignored ->
                          admin.waitPreferredLeaderSynced(
                              replicaLists.keySet(), Duration.ofSeconds(30)))
                  .thenApply(
                      ignored ->
                          new Result(
                              topicName,
                              partitions,
                              replicas,
                              replicaLists.values().stream()
                                  .map(list -> list.get(0))
                                  .collect(Collectors.groupingBy(x -> x, Collectors.counting())),
                              replicaLists.values().stream()
                                  .flatMap(Collection::stream)
                                  .collect(Collectors.groupingBy(x -> x, Collectors.counting()))));
            });
  }

  /** Sample a random replica list from the given probability distribution. */
  public static List<Integer> sampledReplicaList(
      List<Integer> brokerIds, int listSize, IntegerDistribution distribution) {
    if (brokerIds.size() < listSize)
      throw new IllegalStateException(
          "Not enough live brokers to meet the desired replica list size");

    var brokerProbability =
        IntStream.range(0, brokerIds.size())
            .mapToObj(index -> Pair.create(brokerIds.get(index), distribution.probability(index)))
            .collect(Collectors.toList());

    var result = new ArrayList<Integer>();
    while (result.size() < listSize) {
      if (brokerProbability.size() == 1) {
        var remove = brokerProbability.remove(0);
        result.add(remove.getKey());
      } else {
        var enumeratedDistribution = new EnumeratedDistribution<>(brokerProbability);
        var broker = enumeratedDistribution.sample();
        result.add(broker);
        brokerProbability.removeIf(x -> Objects.equals(x.getKey(), broker));
      }
    }
    return result;
  }
}
