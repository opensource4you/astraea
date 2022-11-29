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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Lazy;

public interface ClusterInfo<T extends ReplicaInfo> {
  static <T extends ReplicaInfo> ClusterInfo<T> empty() {
    return of(Set.of(), List.of());
  }

  // ---------------------[helpers]---------------------//

  /**
   * find the changed replicas between `before` and `after`. The diff is based on following
   * conditions. 1) the replicas are existent only in the `before` cluster 2) find the changes based
   * on either broker or data folder between `before` and `after`. Noted that the replicas existent
   * only in the `after` cluster are NOT returned.
   *
   * @param before to be compared
   * @param after to compare
   * @return the diff replicas
   */
  static Set<Replica> diff(ClusterInfo<Replica> before, ClusterInfo<Replica> after) {
    return before
        .replicaStream()
        .parallel()
        .filter(
            beforeReplica ->
                after
                    .replicaStream()
                    .parallel()
                    .noneMatch(
                        r ->
                            r.nodeInfo().id() == beforeReplica.nodeInfo().id()
                                && r.partition() == beforeReplica.partition()
                                && r.topic().equals(beforeReplica.topic())
                                && Objects.equals(r.path(), beforeReplica.path())
                                && r.isLeader() == beforeReplica.isLeader()
                                && r.isPreferredLeader() == beforeReplica.isPreferredLeader()))
        .collect(Collectors.toSet());
  }

  /** Mask specific topics from a {@link ClusterInfo}. */
  static <T extends ReplicaInfo> ClusterInfo<T> masked(
      ClusterInfo<T> clusterInfo, Predicate<String> topicFilter) {
    final var nodes = Set.copyOf(clusterInfo.nodes());
    final var replicas =
        clusterInfo
            .replicaStream()
            .filter(replica -> topicFilter.test(replica.topic()))
            .collect(Collectors.toUnmodifiableList());
    return of(nodes, replicas);
  }

  /**
   * Update the replicas of ClusterInfo according to the given ClusterLogAllocation. The returned
   * {@link ClusterInfo} will have some of its replicas replaced by the replicas inside the given
   * {@link ClusterInfo}. Since {@link ClusterInfo} might only cover a subset of topic/partition in
   * the associated cluster. Only the replicas related to the covered topic/partition get updated.
   *
   * <p>This method intended to offer a way to describe a cluster with some of its state modified
   * manually.
   *
   * @param clusterInfo to get updated
   * @param replacement offers new host and data folder
   * @return new cluster info
   */
  static ClusterInfo<Replica> update(
      ClusterInfo<Replica> clusterInfo, Function<TopicPartition, Collection<Replica>> replacement) {
    var newReplicas =
        clusterInfo.replicas().stream()
            .collect(Collectors.groupingBy(r -> TopicPartition.of(r.topic(), r.partition())))
            .entrySet()
            .stream()
            .map(
                entry -> {
                  var replaced = replacement.apply(entry.getKey());
                  if (replaced.isEmpty()) return entry.getValue();
                  return replaced;
                })
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableList());

    return ClusterInfo.of(clusterInfo.nodes(), newReplicas);
  }

  /**
   * Find a subset of topic/partitions in the source allocation, that has any non-fulfilled log
   * placement in the given target allocation. Note that the given two allocations must have the
   * exactly same topic/partitions set. Otherwise, an {@link IllegalArgumentException} will be
   * raised.
   */
  static Set<TopicPartition> findNonFulfilledAllocation(
      ClusterInfo<Replica> source, ClusterInfo<Replica> target) {

    final var sourceTopicPartition =
        source.replicaStream().map(ReplicaInfo::topicPartition).collect(Collectors.toSet());
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
                    .thenComparing(r -> r.nodeInfo().id()))
            .collect(Collectors.toUnmodifiableList());
    final var targetIds =
        targetReplicas.stream()
            .sorted(
                Comparator.comparing(Replica::isPreferredLeader)
                    .reversed()
                    .thenComparing(r -> r.nodeInfo().id()))
            .collect(Collectors.toUnmodifiableList());
    return IntStream.range(0, sourceIds.size())
        .allMatch(
            index -> {
              final var source = sourceIds.get(index);
              final var target = targetIds.get(index);
              return source.isPreferredLeader() == target.isPreferredLeader()
                  && source.nodeInfo().id() == target.nodeInfo().id()
                  && Objects.equals(source.path(), target.path());
            });
  }

  static String toString(ClusterInfo<Replica> allocation) {
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
                              String.format("(%s, %s) ", log.nodeInfo().id(), log.path())));

              stringBuilder.append(System.lineSeparator());
            });

    return stringBuilder.toString();
  }

  // ---------------------[constructor]---------------------//

  /**
   * convert the kafka Cluster to our ClusterInfo. Noted: this method is used by {@link
   * org.astraea.common.cost.HasBrokerCost} normally, so all data structure are converted
   * immediately
   *
   * @param cluster kafka ClusterInfo
   * @return ClusterInfo
   */
  static ClusterInfo<ReplicaInfo> of(org.apache.kafka.common.Cluster cluster) {
    return of(
        cluster.nodes().stream().map(NodeInfo::of).collect(Collectors.toUnmodifiableSet()),
        cluster.topics().stream()
            .flatMap(t -> cluster.partitionsForTopic(t).stream())
            .flatMap(p -> ReplicaInfo.of(p).stream())
            .collect(Collectors.toUnmodifiableList()));
  }

  /**
   * build a cluster info based on replicas. Noted that the node info are collected by the replicas.
   *
   * <p>Be aware that this class describes <strong>the replica lists of a subset of
   * topic/partitions</strong>. It doesn't require the topic/partition part to have cluster-wide
   * complete information. But the replica list has to be complete. Provide a partial replica list
   * might result in data loss or unintended replica drop during rebalance plan proposing &
   * execution.
   *
   * @param replicas used to build cluster info
   * @return cluster info
   * @param <T> ReplicaInfo or Replica
   */
  static <T extends ReplicaInfo> ClusterInfo<T> of(List<T> replicas) {
    return of(
        replicas.stream().map(ReplicaInfo::nodeInfo).collect(Collectors.toUnmodifiableSet()),
        replicas);
  }

  static <T extends ReplicaInfo> ClusterInfo<T> of(Set<NodeInfo> nodes, List<T> replicas) {
    return new Optimized<>(nodes, replicas);
  }

  // ---------------------[for leader]---------------------//

  static Map<TopicPartition, Long> leaderSize(ClusterInfo<Replica> clusterInfo) {
    return clusterInfo
        .replicaStream()
        .filter(ReplicaInfo::isLeader)
        .collect(
            Collectors.groupingBy(
                ReplicaInfo::topicPartition,
                Collectors.reducing(0L, Replica::size, BinaryOperator.maxBy(Long::compare))));
  }

  /**
   * Get the list of replica leaders
   *
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> replicaLeaders() {
    return replicaStream()
        .filter(ReplicaInfo::isLeader)
        .filter(ReplicaInfo::isOnline)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica leaders of given topic
   *
   * @param topic The Topic name
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> replicaLeaders(String topic) {
    return replicaStream(topic)
        .filter(ReplicaInfo::isLeader)
        .filter(ReplicaInfo::isOnline)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica leaders of given node
   *
   * @param broker the broker id
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> replicaLeaders(int broker) {
    return replicaStream(broker)
        .filter(ReplicaInfo::isLeader)
        .filter(ReplicaInfo::isOnline)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica leaders of given topic on the given node
   *
   * @param broker the broker id
   * @param topic The Topic name
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> replicaLeaders(int broker, String topic) {
    return replicaStream(broker, topic)
        .filter(r -> r.nodeInfo().id() == broker)
        .filter(ReplicaInfo::isLeader)
        .filter(ReplicaInfo::isOnline)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the replica leader of given topic partition
   *
   * @param topicPartition topic and partition id
   * @return {@link ReplicaInfo} or empty if there is no leader
   */
  default Optional<T> replicaLeader(TopicPartition topicPartition) {
    return replicaStream(topicPartition)
        .filter(ReplicaInfo::isLeader)
        .filter(ReplicaInfo::isOnline)
        .findFirst();
  }

  // ---------------------[for available leader]---------------------//

  /**
   * Get the list of available replicas of given topic
   *
   * @param topic The topic name
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> availableReplicas(String topic) {
    return replicaStream(topic)
        .filter(ReplicaInfo::isOnline)
        .collect(Collectors.toUnmodifiableList());
  }

  // ---------------------[for replicas]---------------------//

  /**
   * @return all replicas cached by this cluster info.
   */
  default List<T> replicas() {
    return replicaStream().collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica information of each partition/replica pair for the given topic
   *
   * @param topic The topic name
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> replicas(String topic) {
    return replicaStream(topic).collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica information of each partition/replica pair for the given topic on given
   * broker
   *
   * @param topicPartition topic name and partition id
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> replicas(TopicPartition topicPartition) {
    return replicaStream(topicPartition).collect(Collectors.toUnmodifiableList());
  }

  /**
   * @param replica to search
   * @return the replica matched to input replica
   */
  default Optional<T> replica(TopicPartitionReplica replica) {
    return replicaStream(replica).findFirst();
  }

  // ---------------------[others]---------------------//

  /**
   * All topic names
   *
   * @return return a set of topic names
   */
  default Set<String> topics() {
    return replicaStream().map(ReplicaInfo::topic).collect(Collectors.toUnmodifiableSet());
  }

  default Set<TopicPartition> topicPartitions() {
    return replicaStream().map(ReplicaInfo::topicPartition).collect(Collectors.toUnmodifiableSet());
  }

  default Set<TopicPartitionReplica> topicPartitionReplicas() {
    return replicaStream()
        .map(ReplicaInfo::topicPartitionReplica)
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * find the node associated to node id.
   *
   * @param id node id
   * @return the node information. It throws NoSuchElementException if specify node id is not
   *     associated to any node
   */
  default NodeInfo node(int id) {
    return nodes().stream()
        .filter(n -> n.id() == id)
        .findAny()
        .orElseThrow(() -> new NoSuchElementException(id + " is nonexistent"));
  }

  /**
   * @return the data folders of all nodes. If such information is not available to a specific node,
   *     then an empty set will be associated with that node.
   */
  default Map<Integer, Set<String>> brokerFolders() {
    return nodes().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                NodeInfo::id,
                node -> {
                  if (node instanceof Broker) {
                    return ((Broker) node)
                        .dataFolders().stream()
                            .map(Broker.DataFolder::path)
                            .collect(Collectors.toUnmodifiableSet());
                  } else {
                    return Set.of();
                  }
                }));
  }

  // ---------------------[streams methods]---------------------//
  // implements following methods by smart index to speed up the queries

  default Stream<T> replicaStream(int broker) {
    return replicaStream().filter(r -> r.nodeInfo().id() == broker);
  }

  default Stream<T> replicaStream(String topic) {
    return replicaStream().filter(r -> r.topic().equals(topic));
  }

  default Stream<T> replicaStream(int broker, String topic) {
    return replicaStream(topic).filter(r -> r.nodeInfo().id() == broker);
  }

  default Stream<T> replicaStream(TopicPartition partition) {
    return replicaStream(partition.topic()).filter(r -> r.partition() == partition.partition());
  }

  default Stream<T> replicaStream(TopicPartitionReplica replica) {
    return replicaStream(replica.topicPartition())
        .filter(r -> r.nodeInfo().id() == replica.brokerId());
  }

  // ---------------------[abstract methods]---------------------//

  /**
   * @return The known set of nodes
   */
  Set<NodeInfo> nodes();

  /**
   * @return replica stream to offer effective way to operate a bunch of replicas
   */
  Stream<T> replicaStream();

  /** It optimizes all queries by pre-allocated Map collection. */
  class Optimized<T extends ReplicaInfo> implements ClusterInfo<T> {
    private final Set<NodeInfo> nodeInfos;
    private final List<T> all;

    private final Lazy<Map<Map.Entry<Integer, String>, List<T>>> byBrokerTopic;
    private final Lazy<Map<Integer, List<T>>> byBroker;
    private final Lazy<Map<String, List<T>>> byTopic;
    private final Lazy<Map<TopicPartition, List<T>>> byPartition;
    private final Lazy<Map<TopicPartitionReplica, List<T>>> byReplica;

    protected Optimized(Set<NodeInfo> nodeInfos, List<T> replicas) {
      this.nodeInfos = nodeInfos;
      this.all = replicas;
      this.byBrokerTopic =
          Lazy.of(
              () ->
                  all.stream()
                      .collect(
                          Collectors.groupingBy(
                              r -> Map.entry(r.nodeInfo().id(), r.topic()),
                              Collectors.toUnmodifiableList())));
      this.byBroker =
          Lazy.of(
              () ->
                  all.stream()
                      .collect(
                          Collectors.groupingBy(
                              r -> r.nodeInfo().id(), Collectors.toUnmodifiableList())));

      this.byTopic =
          Lazy.of(
              () ->
                  all.stream()
                      .collect(
                          Collectors.groupingBy(
                              ReplicaInfo::topic, Collectors.toUnmodifiableList())));

      this.byPartition =
          Lazy.of(
              () ->
                  all.stream()
                      .collect(
                          Collectors.groupingBy(
                              ReplicaInfo::topicPartition, Collectors.toUnmodifiableList())));

      this.byReplica =
          Lazy.of(
              () ->
                  all.stream()
                      .collect(
                          Collectors.groupingBy(
                              ReplicaInfo::topicPartitionReplica,
                              Collectors.toUnmodifiableList())));
    }

    @Override
    public Stream<T> replicaStream(String topic) {
      return byTopic.get().getOrDefault(topic, List.of()).stream();
    }

    @Override
    public Stream<T> replicaStream(TopicPartition partition) {
      return byPartition.get().getOrDefault(partition, List.of()).stream();
    }

    @Override
    public Stream<T> replicaStream(TopicPartitionReplica replica) {
      return byReplica.get().getOrDefault(replica, List.of()).stream();
    }

    @Override
    public Stream<T> replicaStream(int broker) {
      return byBroker.get().getOrDefault(broker, List.of()).stream();
    }

    @Override
    public Stream<T> replicaStream(int broker, String topic) {
      return byBrokerTopic.get().getOrDefault(Map.entry(broker, topic), List.of()).stream();
    }

    @Override
    public Set<TopicPartition> topicPartitions() {
      return byPartition.get().keySet();
    }

    @Override
    public Set<TopicPartitionReplica> topicPartitionReplicas() {
      return byReplica.get().keySet();
    }

    @Override
    public Set<String> topics() {
      return byTopic.get().keySet();
    }

    @Override
    public Set<NodeInfo> nodes() {
      return nodeInfos;
    }

    @Override
    public Stream<T> replicaStream() {
      return all.stream();
    }
  }
}
