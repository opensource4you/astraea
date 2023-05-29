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
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Configuration;
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
                          case "clear" -> BalancingModes.CLEAR;
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

  /** Performs common validness checks to the cluster. */
  public static void verifyClearBrokerValidness(ClusterInfo cluster, Predicate<Integer> isClear) {
    var ongoingEventReplica =
        cluster.replicas().stream()
            .filter(r -> isClear.test(r.brokerId()))
            .filter(r -> r.isAdding() || r.isRemoving() || r.isFuture())
            .map(Replica::topicPartitionReplica)
            .collect(Collectors.toUnmodifiableSet());
    if (!ongoingEventReplica.isEmpty())
      throw new IllegalArgumentException(
          "Attempts to clear broker with ongoing migration event (adding/removing/future replica): "
              + ongoingEventReplica);
  }

  /**
   * Move all the replicas at the clearing broker to other allowed brokers. <b>BE CAREFUL, The
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
                            .map(Replica::brokerId)
                            .collect(Collectors.toSet())));
    return ClusterInfo.builder(initial)
        .mapLog(
            replica -> {
              if (!clearBrokers.test(replica.brokerId())) return replica;
              var currentReplicaList = trackingReplicaList.get(replica.topicPartition());
              var broker =
                  IntStream.range(0, allowed.size())
                      .mapToObj(i -> nextBroker.next())
                      .filter(b -> !currentReplicaList.contains(b.id()))
                      .findFirst()
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "Unable to clear replica "
                                      + replica.topicPartitionReplica()
                                      + " for broker "
                                      + replica.brokerId()
                                      + ", the allowed destination brokers are "
                                      + allowed.stream()
                                          .map(Broker::id)
                                          .collect(Collectors.toUnmodifiableSet())
                                      + " but all of them already hosting a replica for this partition. "
                                      + "There is no broker can adopt this replica."));
              var folder = nextBrokerFolder.get(broker.id()).next();

              // update the tracking list. have to do this to avoid putting two replicas from the
              // same tp to one broker.
              currentReplicaList.remove(replica.brokerId());
              currentReplicaList.add(broker.id());

              return Replica.builder(replica).brokerId(broker.id()).path(folder).build();
            })
        .build();
  }

  public static void balancerConfigCheck(Configuration configs, Set<String> supportedConfig) {
    var unsupportedBalancerConfigs =
        configs.raw().keySet().stream()
            .filter(key -> key.startsWith("balancer."))
            .filter(Predicate.not(supportedConfig::contains))
            .collect(Collectors.toSet());
    if (!unsupportedBalancerConfigs.isEmpty())
      throw new IllegalArgumentException(
          "Unsupported balancer configs: "
              + unsupportedBalancerConfigs
              + ", this implementation support "
              + supportedConfig);
  }

  public enum BalancingModes implements EnumInfo {
    BALANCING,
    CLEAR,
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
