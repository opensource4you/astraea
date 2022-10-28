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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.balancer.log.ClusterLogAllocation;

public interface ClusterInfo<T extends ReplicaInfo> {
  ClusterInfo<ReplicaInfo> EMPTY =
      new ClusterInfo<>() {

        @Override
        public Set<NodeInfo> nodes() {
          return Set.of();
        }

        @Override
        public Stream<ReplicaInfo> replicaStream() {
          return Stream.of();
        }
      };

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
                                && r.path().equals(beforeReplica.path())
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
            .collect(Collectors.toList());
    return new ClusterInfo<>() {
      @Override
      public Set<NodeInfo> nodes() {
        return nodes;
      }

      @Override
      public Stream<T> replicaStream() {
        return replicas.stream();
      }
    };
  }

  /**
   * Update the replicas of ClusterInfo according to the given ClusterLogAllocation. The returned
   * {@link ClusterInfo} will have some of its replicas replaced by the replicas inside the given
   * {@link ClusterLogAllocation}. Since {@link ClusterLogAllocation} might only cover a subset of
   * topic/partition in the associated cluster. Only the replicas related to the covered
   * topic/partition get updated.
   *
   * <p>This method intended to offer a way to describe a cluster with some of its state modified
   * manually.
   *
   * @param clusterInfo to get updated
   * @param replacement offers new host and data folder
   * @return new cluster info
   */
  static ClusterInfo<Replica> update(
      ClusterInfo<Replica> clusterInfo, Function<TopicPartition, Set<Replica>> replacement) {
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

  @SuppressWarnings("unchecked")
  static <T extends ReplicaInfo> ClusterInfo<T> empty() {
    return (ClusterInfo<T>) EMPTY;
  }

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
        cluster.nodes().stream().map(NodeInfo::of).collect(Collectors.toSet()),
        cluster.topics().stream()
            .flatMap(t -> cluster.partitionsForTopic(t).stream())
            .flatMap(p -> ReplicaInfo.of(p).stream())
            .collect(Collectors.toUnmodifiableList()));
  }

  /**
   * build a cluster info based on replicas. Noted that the node info are collected by the replicas.
   *
   * @param replicas used to build cluster info
   * @return cluster info
   * @param <T> ReplicaInfo or Replica
   */
  static <T extends ReplicaInfo> ClusterInfo<T> of(List<T> replicas) {
    return of(replicas.stream().map(ReplicaInfo::nodeInfo).collect(Collectors.toSet()), replicas);
  }

  static <T extends ReplicaInfo> ClusterInfo<T> of(Set<NodeInfo> nodes, List<T> replicas) {
    var topics = replicas.stream().map(ReplicaInfo::topic).collect(Collectors.toUnmodifiableSet());
    var replicasForTopic = replicas.stream().collect(Collectors.groupingBy(ReplicaInfo::topic));
    var availableReplicasForTopic =
        replicas.stream()
            .filter(ReplicaInfo::isOnline)
            .collect(Collectors.groupingBy(ReplicaInfo::topic));
    var availableReplicaLeadersForTopics =
        replicas.stream()
            .filter(ReplicaInfo::isOnline)
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.groupingBy(ReplicaInfo::topic));
    // This group is used commonly, so we cache it.
    var availableLeaderReplicasForBrokersTopics =
        replicas.stream()
            .filter(ReplicaInfo::isOnline)
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.groupingBy(r -> Map.entry(r.nodeInfo().id(), r.topic())));

    return new ClusterInfo<>() {
      @Override
      public Set<NodeInfo> nodes() {
        return nodes;
      }

      public Set<String> topics() {
        return topics;
      }

      @Override
      public List<T> replicaLeaders(String topic) {
        return availableReplicaLeadersForTopics.getOrDefault(topic, List.of());
      }

      @Override
      public List<T> replicaLeaders(int broker, String topic) {
        return availableLeaderReplicasForBrokersTopics.getOrDefault(
            Map.entry(broker, topic), List.of());
      }

      @Override
      public List<T> availableReplicas(String topic) {
        return availableReplicasForTopic.getOrDefault(topic, List.of());
      }

      @Override
      public List<T> replicas(String topic) {
        return replicasForTopic.getOrDefault(topic, List.of());
      }

      @Override
      public Stream<T> replicaStream() {
        return replicas.stream();
      }
    };
  }

  // ---------------------[for leader]---------------------//

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
    return replicaStream()
        .filter(r -> r.topic().equals(topic))
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
    return replicaStream()
        .filter(r -> r.nodeInfo().id() == broker)
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
    return replicaStream()
        .filter(r -> r.nodeInfo().id() == broker)
        .filter(r -> r.topic().equals(topic))
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
    return replicaStream()
        .filter(r -> r.topicPartition().equals(topicPartition))
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
    return replicaStream()
        .filter(r -> r.topic().equals(topic))
        .filter(ReplicaInfo::isOnline)
        .collect(Collectors.toUnmodifiableList());
  }

  // ---------------------[for replicas]---------------------//

  /** @return all replicas cached by this cluster info. */
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
    return replicaStream()
        .filter(r -> r.topic().equals(topic))
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica information of each partition/replica pair for the given topic on given
   * broker
   *
   * @param topic The topic name
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> replicas(int broker, String topic) {
    return replicaStream()
        .filter(r -> r.nodeInfo().id() == broker)
        .filter(r -> r.topic().equals(topic))
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica information of each partition/replica pair for the given topic on given
   * broker
   *
   * @param topicPartition topic name and partition id
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> replicas(TopicPartition topicPartition) {
    return replicaStream()
        .filter(r -> r.topicPartition().equals(topicPartition))
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * @param replica to search
   * @return the replica matched to input replica
   */
  default Optional<T> replica(TopicPartitionReplica replica) {
    return replicaStream().filter(r -> r.topicPartitionReplica().equals(replica)).findFirst();
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

  // ---------------------[abstract methods]---------------------//

  /** @return The known set of nodes */
  Set<NodeInfo> nodes();

  /** @return replica stream to offer effective way to operate a bunch of replicas */
  Stream<T> replicaStream();
}
