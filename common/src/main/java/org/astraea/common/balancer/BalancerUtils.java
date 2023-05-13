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

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;

public final class BalancerUtils {

  private BalancerUtils() {}

  /**
   * Verify there is no logic conflict between {@link
   * BalancerConfigs#BALANCER_ALLOWED_TOPICS_REGEX}, {@link
   * BalancerConfigs#BALANCER_ALLOWED_BROKERS_REGEX} and {@link
   * BalancerConfigs#BALANCER_CLEAR_BROKERS_REGEX}. It also performs other common validness check to
   * the cluster.
   */
  public static void verifyClearBrokerValidness(
      ClusterInfo cluster,
      Predicate<Integer> clearBroker,
      Predicate<Integer> allowedBroker,
      Predicate<String> allowedTopics) {
    var disallowedBrokersToClear =
        cluster.nodes().stream()
            .map(NodeInfo::id)
            .filter(Predicate.not(allowedBroker))
            .filter(clearBroker)
            .collect(Collectors.toUnmodifiableSet());
    if (!disallowedBrokersToClear.isEmpty())
      throw new IllegalArgumentException(
          "Attempts to clear the following brokers but they are forbidden from being changed due to \""
              + BalancerConfigs.BALANCER_ALLOWED_BROKERS_REGEX
              + "\": "
              + disallowedBrokersToClear);

    var disallowedTopicsToClear =
        cluster.topicPartitionReplicas().stream()
            .filter(tpr -> clearBroker.test(tpr.brokerId()))
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
            .filter(r -> clearBroker.test(r.nodeInfo().id()))
            .filter(r -> r.isAdding() || r.isRemoving() || r.isFuture())
            .map(Replica::topicPartitionReplica)
            .collect(Collectors.toUnmodifiableSet());
    if (!ongoingEventReplica.isEmpty())
      throw new IllegalArgumentException(
          "Attempts to clear broker with ongoing migration event (adding/removing/future replica): "
              + ongoingEventReplica);
  }

  /**
   * Move all the replicas at the broker to clear to other brokers. <b>BE CAREFUL, The
   * implementation made no assumption for MoveCost or ClusterCost of the returned ClusterInfo.</b>
   * Be aware of this limitation before using it as the starting point for a solution search. Some
   * balancer implementation might have trouble finding answer when starting at a state where the
   * MoveCost is already violated.
   */
  public static ClusterInfo clearedCluster(
      ClusterInfo initial, Predicate<Integer> clearBrokers, Predicate<Integer> allowedBrokers) {
    final var allowed =
        initial.nodes().stream()
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
                            .map(Replica::nodeInfo)
                            .collect(Collectors.toSet())));
    return ClusterInfo.builder(initial)
        .mapLog(
            replica -> {
              if (clearBrokers.test(replica.nodeInfo().id())) {
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
                                        + replica.nodeInfo().id()
                                        + ", the allowed destination brokers are "
                                        + allowed.stream()
                                            .map(NodeInfo::id)
                                            .collect(Collectors.toUnmodifiableSet())
                                        + " but all of them already hosting a replica for this partition. "
                                        + "There is no broker can adopt this replica."));
                var folder = nextBrokerFolder.get(broker.id()).next();

                // update the tracking list. have to do this to avoid putting two replicas from the
                // same tp to one broker.
                currentReplicaList.remove(replica.nodeInfo());
                currentReplicaList.add(broker);

                return Replica.builder(replica).nodeInfo(broker).path(folder).build();
              } else {
                return replica;
              }
            })
        .build();
  }
}
