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
package org.astraea.common.balancer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.EnumInfo;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;

public final class BalancerUtils {

  private BalancerUtils() {}

  public static Map<Integer, BalancingModes> balancingMode(ClusterInfo cluster, String config) {
    var num = Pattern.compile("[0-9]+");

    var map =
        Arrays.stream(config.split(","))
            .filter(Predicate.not(String::isEmpty))
            .map(x -> x.split(":"))
            .collect(
                Collectors.toUnmodifiableMap(
                    s -> (Object) (num.matcher(s[0]).find() ? Integer.parseInt(s[0]) : s[0]),
                    s ->
                        switch (s[1]) {
                          case "balancing" -> BalancingModes.BALANCING;
                          case "demoted" -> BalancingModes.DEMOTED;
                          case "excluded" -> BalancingModes.EXCLUDED;
                          default -> throw new IllegalArgumentException(
                              "Unsupported balancing mode: " + s[1]);
                        }));

    Function<Integer, BalancingModes> mode =
        (id) -> map.getOrDefault(id, map.getOrDefault("default", BalancingModes.BALANCING));

    return cluster.brokers().stream()
        .map(Broker::id)
        .collect(Collectors.toUnmodifiableMap(Function.identity(), mode));
  }

  /**
   * Verify there is no logic conflict between {@link BalancerConfigs#BALANCER_ALLOWED_TOPICS_REGEX}
   * and {@link BalancerConfigs#BALANCER_BROKER_BALANCING_MODE}. It also performs other common
   * validness checks to the cluster.
   */
  public static void verifyClearBrokerValidness(
      ClusterInfo cluster, Predicate<Integer> isDemoted, Predicate<String> allowedTopics) {
    var disallowedTopicsToClear =
        cluster.topicPartitionReplicas().stream()
            .filter(tpr -> isDemoted.test(tpr.brokerId()))
            .filter(tpr -> !allowedTopics.test(tpr.topic()))
            .collect(Collectors.toUnmodifiableSet());
    if (!disallowedTopicsToClear.isEmpty())
      throw new IllegalArgumentException(
          "Attempts to clear some brokers, but some of them contain topics that forbidden from being changed due to \""
              + BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX
              + "\": "
              + disallowedTopicsToClear);

    var ongoingEventReplica =
        cluster.replicas().stream()
            .filter(r -> isDemoted.test(r.broker().id()))
            .filter(r -> r.isAdding() || r.isRemoving() || r.isFuture())
            .map(Replica::topicPartitionReplica)
            .collect(Collectors.toUnmodifiableSet());
    if (!ongoingEventReplica.isEmpty())
      throw new IllegalArgumentException(
          "Attempts to clear broker with ongoing migration event (adding/removing/future replica): "
              + ongoingEventReplica);
  }

  /**
   * Move all the replicas at the demoting broker to other allowed brokers. <b>BE CAREFUL, The
   * implementation made no assumption for MoveCost or ClusterCost of the returned ClusterInfo.</b>
   * Be aware of this limitation before using it as the starting point for a solution search. Some
   * balancer implementation might have trouble finding answer when starting at a state where the
   * MoveCost is already violated.
   */
  public static ClusterInfo clearedCluster(
      ClusterInfo initial, Predicate<Integer> clearBrokers, Predicate<Integer> allowedBrokers) {
    final var allowed =
        initial.brokers().stream()
            .filter(node -> allowedBrokers.test(node.id()))
            .filter(node -> Predicate.not(clearBrokers).test(node.id()))
            .collect(Collectors.toUnmodifiableSet());
    final var nextBroker = Stream.generate(() -> allowed).flatMap(Collection::stream).iterator();
    final var nextBrokerFolder =
        initial.brokerFolders().entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Map.Entry::getKey,
                    x -> Stream.generate(x::getValue).flatMap(Collection::stream).iterator()));

    var trackingReplicaList =
        initial.topicPartitions().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    tp -> tp,
                    tp ->
                        initial.replicas(tp).stream()
                            .map(Replica::broker)
                            .collect(Collectors.toSet())));
    return ClusterInfo.builder(initial)
        .mapLog(
            replica -> {
              if (!clearBrokers.test(replica.broker().id())) return replica;
              var currentReplicaList = trackingReplicaList.get(replica.topicPartition());
              var broker =
                  IntStream.range(0, allowed.size())
                      .mapToObj(i -> nextBroker.next())
                      .filter(b -> !currentReplicaList.contains(b))
                      .findFirst()
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "Unable to clear replica "
                                      + replica.topicPartitionReplica()
                                      + " for broker "
                                      + replica.broker().id()
                                      + ", the allowed destination brokers are "
                                      + allowed.stream()
                                          .map(Broker::id)
                                          .collect(Collectors.toUnmodifiableSet())
                                      + " but all of them already hosting a replica for this partition. "
                                      + "There is no broker can adopt this replica."));
              var folder = nextBrokerFolder.get(broker.id()).next();

              // update the tracking list. have to do this to avoid putting two replicas from the
              // same tp to one broker.
              currentReplicaList.remove(replica.broker());
              currentReplicaList.add(broker);

              return Replica.builder(replica).broker(broker).path(folder).build();
            })
        .build();
  }

  public enum BalancingModes implements EnumInfo {
    BALANCING,
    DEMOTED,
    EXCLUDED;

    public static BalancingModes ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(BalancingModes.class, alias);
    }

    @Override
    public String alias() {
      return name();
    }

    @Override
    public String toString() {
      return alias();
    }
  }
}
