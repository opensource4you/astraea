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
package org.astraea.app.admin;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public interface ClusterInfo<T extends ReplicaInfo> {
  ClusterInfo<ReplicaInfo> EMPTY =
      new ClusterInfo<>() {

        @Override
        public List<NodeInfo> nodes() {
          return List.of();
        }

        @Override
        public List<ReplicaInfo> replicas() {
          return List.of();
        }
      };

  /**
   * compare the replicas according "data folder"
   *
   * @param before to be compared
   * @param after to compare
   * @return the diff replicas
   */
  static Set<Replica> diff4DataFolder(ClusterInfo<Replica> before, ClusterInfo<Replica> after) {
    return ClusterInfo.diff(before, after, (b, a) -> b.dataFolder().equals(a.dataFolder()));
  }

  /**
   * find the changed replicas between `before` and `after`. The diff is based on following
   * conditions. 1) the replicas are existent only in the `before` cluster 2) the replicas existent
   * on both `before` and `after` are evaluated to be "not equal" Noted that the replicas existent
   * only in the `after` cluster are NOT returned.
   *
   * @param before to be compared
   * @param after to compare
   * @param equal to compare replica
   * @return the diff replicas
   */
  static Set<Replica> diff(
      ClusterInfo<Replica> before,
      ClusterInfo<Replica> after,
      BiFunction<Replica, Replica, Boolean> equal) {
    return before.replicas().stream()
        .filter(
            beforeReplica ->
                after
                    .replica(beforeReplica.topicPartitionReplica())
                    // not equal so it is changed
                    .map(newReplica -> !equal.apply(beforeReplica, newReplica))
                    // no replica in the after cluster so it is changed
                    .orElse(true))
        .collect(Collectors.toSet());
  }

  @SuppressWarnings("unchecked")
  static <T extends ReplicaInfo> ClusterInfo<T> empty() {
    return (ClusterInfo<T>) EMPTY;
  }

  /**
   * convert the kafka Cluster to our ClusterInfo. Noted: this method is used by {@link
   * org.astraea.app.cost.HasBrokerCost} normally, so all data structure are converted immediately
   *
   * @param cluster kafka ClusterInfo
   * @return ClusterInfo
   */
  static ClusterInfo<ReplicaInfo> of(org.apache.kafka.common.Cluster cluster) {
    return of(
        cluster.nodes().stream().map(NodeInfo::of).collect(Collectors.toUnmodifiableList()),
        cluster.topics().stream()
            .flatMap(t -> cluster.partitionsForTopic(t).stream())
            .flatMap(p -> ReplicaInfo.of(p).stream())
            .collect(Collectors.toUnmodifiableList()));
  }

  static <T extends ReplicaInfo> ClusterInfo<T> of(List<NodeInfo> nodes, List<T> replicas) {
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
      public List<NodeInfo> nodes() {
        return nodes;
      }

      public Set<String> topics() {
        return topics;
      }

      @Override
      public List<T> availableReplicaLeaders(String topic) {
        return availableReplicaLeadersForTopics.getOrDefault(topic, List.of());
      }

      @Override
      public List<T> availableReplicaLeaders(int broker, String topic) {
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
      public List<T> replicas() {
        return replicas;
      }
    };
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
   * Get the list of replica leader information of each available partition for the given topic
   *
   * @param topic The Topic name
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> availableReplicaLeaders(String topic) {
    return replicas(topic).stream()
        .filter(ReplicaInfo::isLeader)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica leader information of each available partition for the given
   * broker/topic
   *
   * @param broker the broker id
   * @param topic The Topic name
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> availableReplicaLeaders(int broker, String topic) {
    return availableReplicaLeaders(topic).stream()
        .filter(r -> r.nodeInfo().id() == broker)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica information of each available partition/replica pair for the given
   * topic
   *
   * @param topic The topic name
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> availableReplicas(String topic) {
    return replicas(topic).stream()
        .filter(ReplicaInfo::isOnline)
        .collect(Collectors.toUnmodifiableList());
  }

  /** @return The known set of nodes */
  List<NodeInfo> nodes();

  /**
   * All topic names
   *
   * @return return a set of topic names
   */
  default Set<String> topics() {
    return replicas().stream().map(ReplicaInfo::topic).collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Get the list of replica information of each partition/replica pair for the given topic
   *
   * @param topic The topic name
   * @return A list of {@link ReplicaInfo}.
   */
  default List<T> replicas(String topic) {
    return replicas().stream()
        .filter(r -> r.topic().equals(topic))
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * @param replica to search
   * @return the replica matched to input replica
   */
  default Optional<T> replica(TopicPartitionReplica replica) {
    return replicas().stream().filter(r -> r.topicPartitionReplica().equals(replica)).findFirst();
  }

  /** @return all replicas cached by this cluster info. */
  List<T> replicas();
}
