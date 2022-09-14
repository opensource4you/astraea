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
package org.astraea.app.scenario.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.util.Pair;
import org.astraea.app.scenario.Scenario;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.cost.Configuration;

public class SkewedPartitionScenario implements Scenario<SkewedPartitionScenario.Result> {

  final String topicName;
  final int partitions;
  final short replicas;
  final double binomialProbability;

  public SkewedPartitionScenario(Configuration configuration) {
    this(
        configuration.string("topicName").orElse(Utils.randomString()),
        configuration.string("partitions").map(Integer::parseInt).orElse(10),
        configuration.string("replicas").map(Short::parseShort).orElse((short) 1),
        configuration.string("binomialProbability").map(Double::parseDouble).orElse(0.5));
  }

  private SkewedPartitionScenario(
      String topicName, int partitions, short replicas, double binomialProbability) {
    this.topicName = topicName;
    this.partitions = partitions;
    this.replicas = replicas;
    this.binomialProbability = binomialProbability;
  }

  @Override
  public Result apply(Admin admin) {
    // retrieve online brokers
    var brokers = admin.brokerIds().stream().sorted().collect(Collectors.toUnmodifiableList());

    // create topic
    admin
        .creator()
        .topic(topicName)
        .numberOfPartitions(partitions)
        .numberOfReplicas(replicas)
        .create();
    Utils.sleep(Duration.ofSeconds(1));

    var distribution = new BinomialDistribution(brokers.size() - 1, binomialProbability);
    var replicaLists =
        IntStream.range(0, partitions)
            .boxed()
            .collect(
                Collectors.toUnmodifiableMap(
                    p -> TopicPartition.of(topicName, p),
                    ignore -> sampledReplicaList(brokers, replicas, distribution)));

    replicaLists.forEach(
        (tp, newReplicas) ->
            admin.migrator().partition(tp.topic(), tp.partition()).moveTo(newReplicas));
    Utils.sleep(Duration.ofSeconds(1));

    // elect leader
    replicaLists.keySet().forEach(admin::preferredLeaderElection);
    Utils.sleep(Duration.ofSeconds(1));

    return new Result(
        topicName,
        partitions,
        replicas,
        binomialProbability,
        replicaLists.values().stream()
            .map(list -> list.get(0))
            .collect(Collectors.groupingBy(x -> x, Collectors.counting())),
        replicaLists.values().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.groupingBy(x -> x, Collectors.counting())));
  }

  /** Sample a random replica list from the given probability distribution. */
  static List<Integer> sampledReplicaList(
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

  public static class Result {

    public final String topicName;
    public final int partitions;
    public final short replicas;
    public final double binomialProbability;
    public final Map<Integer, Long> leaderSum;
    public final Map<Integer, Long> logSum;

    public Result(
        String topicName,
        int partitions,
        short replicas,
        double binomialProbability,
        Map<Integer, Long> leaderSum,
        Map<Integer, Long> logSum) {
      this.topicName = topicName;
      this.partitions = partitions;
      this.replicas = replicas;
      this.binomialProbability = binomialProbability;
      this.leaderSum = leaderSum;
      this.logSum = logSum;
    }
  }
}
