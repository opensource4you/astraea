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
import java.util.Set;
import java.util.stream.Collectors;

public interface ClusterInfo {
  ClusterInfo EMPTY =
      new ClusterInfo() {

        @Override
        public List<NodeInfo> nodes() {
          return List.of();
        }

        @Override
        public Set<String> dataDirectories(int brokerId) {
          return Set.of();
        }

        @Override
        public List<ReplicaInfo> availableReplicaLeaders(String topic) {
          return List.of();
        }

        @Override
        public List<ReplicaInfo> availableReplicas(String topic) {
          return List.of();
        }

        @Override
        public Set<String> topics() {
          return Set.of();
        }

        @Override
        public List<ReplicaInfo> replicas(String topic) {
          return List.of();
        }
      };

  /**
   * convert the kafka Cluster to our ClusterInfo. All data structure are converted immediately, so
   * you should cache the result if the performance is critical
   *
   * @param cluster kafka ClusterInfo
   * @return astraea ClusterInfo
   */
  static ClusterInfo of(org.apache.kafka.common.Cluster cluster) {
    var nodes = cluster.nodes().stream().map(NodeInfo::of).collect(Collectors.toUnmodifiableList());
    var topics = cluster.topics();
    var allReplicas =
        topics.stream()
            .flatMap(t -> cluster.partitionsForTopic(t).stream())
            .flatMap(p -> ReplicaInfo.of(p).stream())
            .collect(Collectors.toUnmodifiableList());
    var replicasForTopic = allReplicas.stream().collect(Collectors.groupingBy(ReplicaInfo::topic));
    var availableReplicasForTopic =
        allReplicas.stream()
            .filter(ReplicaInfo::isOnlineReplica)
            .collect(Collectors.groupingBy(ReplicaInfo::topic));
    var availableReplicaLeadersForTopics =
        allReplicas.stream()
            .filter(ReplicaInfo::isOnlineReplica)
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.groupingBy(ReplicaInfo::topic));
    // This group is used commonly, so we cached it.
    var availableLeaderReplicasForBrokersTopics =
        allReplicas.stream()
            .filter(ReplicaInfo::isOnlineReplica)
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.groupingBy(r -> Map.entry(r.nodeInfo().id(), r.topic())));

    return new ClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return nodes;
      }

      @Override
      public Set<String> dataDirectories(int brokerId) {
        // org.apache.kafka.common.Cluster doesn't have such information.
        throw new UnsupportedOperationException("This information is not available");
      }

      public Set<String> topics() {
        return topics;
      }

      @Override
      public List<ReplicaInfo> availableReplicaLeaders(String topic) {
        return availableReplicaLeadersForTopics.getOrDefault(topic, List.of());
      }

      @Override
      public List<ReplicaInfo> availableReplicaLeaders(int broker, String topic) {
        return availableLeaderReplicasForBrokersTopics.getOrDefault(
            Map.entry(broker, topic), List.of());
      }

      @Override
      public List<ReplicaInfo> availableReplicas(String topic) {
        return availableReplicasForTopic.getOrDefault(topic, List.of());
      }

      @Override
      public List<ReplicaInfo> replicas(String topic) {
        return replicasForTopic.getOrDefault(topic, List.of());
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

  /** @return The known set of nodes */
  List<NodeInfo> nodes();

  /**
   * @return return the data directories on specific broker. It throws NoSuchElementException if
   *     specify node id is not associated to any node.
   */
  Set<String> dataDirectories(int brokerId);

  /**
   * Get the list of replica leader information of each available partition for the given topic
   *
   * @param topic The Topic name
   * @return A list of {@link ReplicaInfo}. It throws NoSuchElementException if the replica info of
   *     the given topic is unknown to this ClusterInfo
   */
  List<ReplicaInfo> availableReplicaLeaders(String topic);

  /**
   * Get the list of replica leader information of each available partition for the given
   * broker/topic
   *
   * @param broker the broker id
   * @param topic The Topic name
   * @return A list of {@link ReplicaInfo}. It throws NoSuchElementException if the replica info of
   *     the given topic is unknown to this ClusterInfo
   */
  default List<ReplicaInfo> availableReplicaLeaders(int broker, String topic) {
    return availableReplicaLeaders(topic).stream()
        .filter(r -> r.nodeInfo().id() == broker)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get the list of replica information of each available partition/replica pair for the given
   * topic
   *
   * @param topic The topic name
   * @return A list of {@link ReplicaInfo}. It throws NoSuchElementException if the replica info of
   *     the given topic is unknown to this ClusterInfo
   */
  List<ReplicaInfo> availableReplicas(String topic);

  /**
   * All topic names
   *
   * @return return a set of topic names
   */
  Set<String> topics();

  /**
   * Get the list of replica information of each partition/replica pair for the given topic
   *
   * @param topic The topic name
   * @return A list of {@link ReplicaInfo}. It throws NoSuchElementException if the replica info of
   *     the given topic is unknown to this ClusterInfo
   */
  List<ReplicaInfo> replicas(String topic);
}
