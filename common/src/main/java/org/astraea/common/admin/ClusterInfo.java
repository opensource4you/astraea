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
package org.astraea.common.admin;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface ClusterInfo {

  static ClusterInfoBuilder builder() {
    return builder(empty());
  }

  static ClusterInfoBuilder builder(ClusterInfo clusterInfo) {
    return new ClusterInfoBuilder(clusterInfo);
  }

  static ClusterInfo empty() {
    return of("unknown", List.of(), Map.of(), List.of());
  }

  /**
   * Find a subset of topic/partitions in the source allocation, that has any non-fulfilled log
   * placement in the given target allocation. Note that the given two allocations must have the
   * exactly same topic/partitions set. Otherwise, an {@link IllegalArgumentException} will be
   * raised.
   */
  static Set<TopicPartition> findNonFulfilledAllocation(ClusterInfo source, ClusterInfo target) {

    final var sourceTopicPartition =
        source.replicaStream().map(Replica::topicPartition).collect(Collectors.toSet());
    final var targetTopicPartition = target.topicPartitions();
    final var unknownTopicPartitions =
        targetTopicPartition.stream()
            .filter(tp -> !sourceTopicPartition.contains(tp))
            .collect(Collectors.toUnmodifiableSet());

    if (!unknownTopicPartitions.isEmpty())
      throw new IllegalArgumentException(
          "target topic/partition should be a subset of source topic/partition: "
              + unknownTopicPartitions);

    return targetTopicPartition.stream()
        .filter(tp -> !placementMatch(source.replicas(tp), target.replicas(tp)))
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Determine if both of the replicas can be considered as equal in terms of its placement.
   *
   * @param sourceReplicas the source replicas. null value of {@link Replica#path()} will be
   *     interpreted as the actual location doesn't matter.
   * @param targetReplicas the target replicas. null value of {@link Replica#path()} will be
   *     interpreted as the actual location is unknown.
   * @return true if both replicas of specific topic/partitions can be considered as equal in terms
   *     of its placement.
   */
  static boolean placementMatch(
      Collection<Replica> sourceReplicas, Collection<Replica> targetReplicas) {
    if (sourceReplicas.size() != targetReplicas.size()) return false;
    final var sourceIds =
        sourceReplicas.stream()
            .sorted(
                Comparator.comparing(Replica::isPreferredLeader)
                    .reversed()
                    .thenComparing(Replica::brokerId))
            .toList();
    final var targetIds =
        targetReplicas.stream()
            .sorted(
                Comparator.comparing(Replica::isPreferredLeader)
                    .reversed()
                    .thenComparing(Replica::brokerId))
            .toList();
    return IntStream.range(0, sourceIds.size())
        .allMatch(
            index -> {
              final var source = sourceIds.get(index);
              final var target = targetIds.get(index);
              return source.isPreferredLeader() == target.isPreferredLeader()
                  && source.brokerId() == target.brokerId()
                  && Objects.equals(source.path(), target.path());
            });
  }

  static String toString(ClusterInfo allocation) {
    StringBuilder stringBuilder = new StringBuilder();

    allocation.topicPartitions().stream()
        .sorted()
        .forEach(
            tp -> {
              stringBuilder.append("[").append(tp).append("] ");

              allocation
                  .replicas(tp)
                  .forEach(
                      log ->
                          stringBuilder.append(
                              String.format("(%s, %s) ", log.brokerId(), log.path())));

              stringBuilder.append(System.lineSeparator());
            });

    return stringBuilder.toString();
  }

  // ---------------------[constructor]---------------------//

  /**
   * build a cluster info based on replicas and a set of cluster node information.
   *
   * <p>Be aware that this the <code>replicas</code>parameter describes <strong>the replica lists of
   * a subset of topic/partitions</strong>. It doesn't require the topic/partition part to have
   * cluster-wide complete information. But the replica list has to be complete. Provide a partial
   * replica list might result in data loss or unintended replica drop during rebalance plan
   * proposing & execution.
   *
   * @param clusterId the id of the Kafka cluster
   * @param nodes the node information of the cluster info
   * @param topics topics
   * @param replicas used to build cluster info
   * @return cluster info
   */
  static ClusterInfo of(
      String clusterId, List<Broker> nodes, Map<String, Topic> topics, List<Replica> replicas) {
    return new OptimizedClusterInfo(clusterId, nodes, topics, replicas);
  }

  // ---------------------[for leader]---------------------//

  static Map<TopicPartition, Long> leaderSize(ClusterInfo clusterInfo) {
    return clusterInfo
        .replicaStream()
        .filter(Replica::isLeader)
        .collect(
            Collectors.groupingBy(
                Replica::topicPartition,
                Collectors.reducing(0L, Replica::size, BinaryOperator.maxBy(Long::compare))));
  }

  /**
   * Get the list of replica leaders
   *
   * @return A list of {@link Replica}.
   */
  default List<Replica> replicaLeaders() {
    return replicaStream()
        .filter(Replica::isLeader)
        .filter(Replica::isOnline)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica leaders of given topic
   *
   * @param topic The Topic name
   * @return A list of {@link Replica}.
   */
  default List<Replica> replicaLeaders(String topic) {
    return replicaStream(topic)
        .filter(Replica::isLeader)
        .filter(Replica::isOnline)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica leaders of given node
   *
   * @param broker the broker id
   * @return A list of {@link Replica}.
   */
  default List<Replica> replicaLeaders(int broker) {
    return replicaStream(broker)
        .filter(Replica::isLeader)
        .filter(Replica::isOnline)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica leaders of given topic on the given node
   *
   * @return A list of {@link Replica}.
   */
  default List<Replica> replicaLeaders(BrokerTopic brokerTopic) {
    return replicaStream(brokerTopic)
        .filter(Replica::isLeader)
        .filter(Replica::isOnline)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the replica leader of given topic partition
   *
   * @param topicPartition topic and partition id
   * @return {@link Replica} or empty if there is no leader
   */
  default Optional<Replica> replicaLeader(TopicPartition topicPartition) {
    return replicaStream(topicPartition)
        .filter(Replica::isLeader)
        .filter(Replica::isOnline)
        .findFirst();
  }

  // ---------------------[for available leader]---------------------//

  /**
   * Get the list of available replicas of given topic
   *
   * @param topic The topic name
   * @return A list of {@link Replica}.
   */
  default List<Replica> availableReplicas(String topic) {
    return replicaStream(topic).filter(Replica::isOnline).collect(Collectors.toUnmodifiableList());
  }

  // ---------------------[for replicas]---------------------//

  /**
   * @return all replicas cached by this cluster info.
   */
  default List<Replica> replicas() {
    return replicaStream().collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica information of each partition/replica pair for the given topic
   *
   * @param topic The topic name
   * @return A list of {@link Replica}.
   */
  default List<Replica> replicas(String topic) {
    return replicaStream(topic).collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica information of each partition/replica pair for the given topic on given
   * broker
   *
   * @param topicPartition topic name and partition id
   * @return A list of {@link Replica}.
   */
  default List<Replica> replicas(TopicPartition topicPartition) {
    return replicaStream(topicPartition).collect(Collectors.toUnmodifiableList());
  }

  /**
   * @param replica to search
   * @return the replica matched to input replica
   */
  default List<Replica> replicas(TopicPartitionReplica replica) {
    return replicaStream(replica).collect(Collectors.toUnmodifiableList());
  }

  // ---------------------[others]---------------------//

  String clusterId();

  /**
   * All topic names
   *
   * @return return a set of topic names
   */
  default Set<String> topicNames() {
    return replicaStream().map(Replica::topic).collect(Collectors.toUnmodifiableSet());
  }

  default Set<TopicPartition> topicPartitions() {
    return replicaStream().map(Replica::topicPartition).collect(Collectors.toUnmodifiableSet());
  }

  default Set<TopicPartitionReplica> topicPartitionReplicas() {
    return replicaStream()
        .map(Replica::topicPartitionReplica)
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * find the node associated to node id.
   *
   * @param id node id
   * @return the node information. It throws NoSuchElementException if specify node id is not
   *     associated to any node
   */
  default Broker node(int id) {
    return brokers().stream()
        .filter(n -> n.id() == id)
        .findAny()
        .orElseThrow(() -> new NoSuchElementException(id + " is nonexistent"));
  }

  /**
   * @return the data folders of all nodes. If such information is not available to a specific node,
   *     then an empty set will be associated with that node.
   */
  default Map<Integer, Set<String>> brokerFolders() {
    return brokers().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                Broker::id,
                node ->
                    node.dataFolders().stream()
                        .map(Broker.DataFolder::path)
                        .collect(Collectors.toUnmodifiableSet())));
  }

  // ---------------------[streams methods]---------------------//
  // implements following methods by smart index to speed up the queries

  default Stream<Replica> replicaStream(int broker) {
    return replicaStream().filter(r -> r.brokerId() == broker);
  }

  default Stream<Replica> replicaStream(String topic) {
    return replicaStream().filter(r -> r.topic().equals(topic));
  }

  default Stream<Replica> replicaStream(BrokerTopic brokerTopic) {
    return replicaStream(brokerTopic.topic()).filter(r -> r.brokerId() == brokerTopic.broker());
  }

  default Stream<Replica> replicaStream(TopicPartition partition) {
    return replicaStream(partition.topic()).filter(r -> r.partition() == partition.partition());
  }

  default Stream<Replica> replicaStream(TopicPartitionReplica replica) {
    return replicaStream(replica.topicPartition()).filter(r -> r.brokerId() == replica.brokerId());
  }

  // ---------------------[abstract methods]---------------------//

  /**
   * @return The known brokers
   */
  List<Broker> brokers();

  /**
   * @return replica stream to offer effective way to operate a bunch of replicas
   */
  Stream<Replica> replicaStream();

  /**
   * @return a map of topic description
   */
  Map<String, Topic> topics();
}
