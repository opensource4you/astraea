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

public interface ClusterInfo {
  static ClusterInfo empty() {
    return of("unknown", List.of(), List.of());
  }

  // ---------------------[helpers]---------------------//

  /** Mask specific topics from a {@link ClusterInfo}. */
  static ClusterInfo masked(ClusterInfo clusterInfo, Predicate<String> topicFilter) {
    final var nodes = List.copyOf(clusterInfo.nodes());
    final var replicas =
        clusterInfo
            .replicaStream()
            .filter(replica -> topicFilter.test(replica.topic()))
            .collect(Collectors.toUnmodifiableList());
    return of(clusterInfo.clusterId(), nodes, replicas);
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
  static ClusterInfo update(
      ClusterInfo clusterInfo, Function<TopicPartition, Collection<Replica>> replacement) {
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

    return ClusterInfo.of(clusterInfo.clusterId(), clusterInfo.nodes(), newReplicas);
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
                              String.format("(%s, %s) ", log.nodeInfo().id(), log.path())));

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
   * @param nodes the node information of the cluster info
   * @param replicas used to build cluster info
   * @return cluster info
   */
  static ClusterInfo of(String clusterId, List<NodeInfo> nodes, List<Replica> replicas) {
    return new Optimized(clusterId, nodes, replicas);
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
  default Set<String> topics() {
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
    return brokers().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                NodeInfo::id,
                node ->
                    node.dataFolders().stream()
                        .map(Broker.DataFolder::path)
                        .collect(Collectors.toUnmodifiableSet())));
  }

  // ---------------------[streams methods]---------------------//
  // implements following methods by smart index to speed up the queries

  default Stream<Replica> replicaStream(int broker) {
    return replicaStream().filter(r -> r.nodeInfo().id() == broker);
  }

  default Stream<Replica> replicaStream(String topic) {
    return replicaStream().filter(r -> r.topic().equals(topic));
  }

  default Stream<Replica> replicaStream(BrokerTopic brokerTopic) {
    return replicaStream(brokerTopic.topic())
        .filter(r -> r.nodeInfo().id() == brokerTopic.broker());
  }

  default Stream<Replica> replicaStream(TopicPartition partition) {
    return replicaStream(partition.topic()).filter(r -> r.partition() == partition.partition());
  }

  default Stream<Replica> replicaStream(TopicPartitionReplica replica) {
    return replicaStream(replica.topicPartition())
        .filter(r -> r.nodeInfo().id() == replica.brokerId());
  }

  // ---------------------[abstract methods]---------------------//

  /**
   * @return The known nodes
   */
  List<NodeInfo> nodes();

  default List<Broker> brokers() {
    return nodes().stream()
        .filter(n -> n instanceof Broker)
        .map(n -> (Broker) n)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * @return replica stream to offer effective way to operate a bunch of replicas
   */
  Stream<Replica> replicaStream();

  /** It optimizes all queries by pre-allocated Map collection. */
  class Optimized implements ClusterInfo {
    private final String clusterId;
    private final List<NodeInfo> nodeInfos;
    private final List<Replica> all;

    private final Lazy<Map<BrokerTopic, List<Replica>>> byBrokerTopic;

    private final Lazy<Map<BrokerTopic, List<Replica>>> byBrokerTopicForLeader;
    private final Lazy<Map<Integer, List<Replica>>> byBroker;
    private final Lazy<Map<String, List<Replica>>> byTopic;

    private final Lazy<Map<String, List<Replica>>> byTopicForLeader;
    private final Lazy<Map<TopicPartition, List<Replica>>> byPartition;
    private final Lazy<Map<TopicPartitionReplica, List<Replica>>> byReplica;

    protected Optimized(String clusterId, List<NodeInfo> nodeInfos, List<Replica> replicas) {
      this.clusterId = clusterId;
      this.nodeInfos = nodeInfos;
      this.all = replicas;
      this.byBrokerTopic =
          Lazy.of(
              () ->
                  all.stream()
                      .collect(
                          Collectors.groupingBy(
                              r -> BrokerTopic.of(r.nodeInfo().id(), r.topic()),
                              Collectors.toUnmodifiableList())));
      this.byBrokerTopicForLeader =
          Lazy.of(
              () ->
                  all.stream()
                      .filter(Replica::isOnline)
                      .filter(Replica::isLeader)
                      .collect(
                          Collectors.groupingBy(
                              r -> BrokerTopic.of(r.nodeInfo().id(), r.topic()),
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
                          Collectors.groupingBy(Replica::topic, Collectors.toUnmodifiableList())));

      this.byTopicForLeader =
          Lazy.of(
              () ->
                  all.stream()
                      .filter(Replica::isOnline)
                      .filter(Replica::isLeader)
                      .collect(
                          Collectors.groupingBy(Replica::topic, Collectors.toUnmodifiableList())));

      this.byPartition =
          Lazy.of(
              () ->
                  all.stream()
                      .collect(
                          Collectors.groupingBy(
                              Replica::topicPartition, Collectors.toUnmodifiableList())));

      this.byReplica =
          Lazy.of(
              () ->
                  all.stream()
                      .collect(
                          Collectors.groupingBy(
                              Replica::topicPartitionReplica, Collectors.toUnmodifiableList())));
    }

    @Override
    public Stream<Replica> replicaStream(String topic) {
      return byTopic.get().getOrDefault(topic, List.of()).stream();
    }

    @Override
    public Stream<Replica> replicaStream(TopicPartition partition) {
      return byPartition.get().getOrDefault(partition, List.of()).stream();
    }

    @Override
    public Stream<Replica> replicaStream(TopicPartitionReplica replica) {
      return byReplica.get().getOrDefault(replica, List.of()).stream();
    }

    @Override
    public Stream<Replica> replicaStream(int broker) {
      return byBroker.get().getOrDefault(broker, List.of()).stream();
    }

    @Override
    public Stream<Replica> replicaStream(BrokerTopic brokerTopic) {
      return byBrokerTopic.get().getOrDefault(brokerTopic, List.of()).stream();
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
    public String clusterId() {
      return clusterId;
    }

    @Override
    public Set<String> topics() {
      return byTopic.get().keySet();
    }

    @Override
    public List<NodeInfo> nodes() {
      return nodeInfos;
    }

    @Override
    public Stream<Replica> replicaStream() {
      return all.stream();
    }

    @Override
    public List<Replica> replicaLeaders(String topic) {
      return byTopicForLeader.get().getOrDefault(topic, List.of());
    }

    @Override
    public List<Replica> replicaLeaders(BrokerTopic brokerTopic) {
      return byBrokerTopicForLeader.get().getOrDefault(brokerTopic, List.of());
    }
  }
}
