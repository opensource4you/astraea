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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.metrics.HasBeanObject;

public interface ClusterInfo {

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
    var replicas =
        topics.stream()
            .flatMap(t -> cluster.availablePartitionsForTopic(t).stream())
            .flatMap(p -> ReplicaInfo.of(p).stream())
            .collect(Collectors.toUnmodifiableList());
    var availableReplicas = replicas.stream().collect(Collectors.groupingBy(ReplicaInfo::topic));
    var availableReplicaLeaders =
        availableReplicas.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        e.getValue().stream()
                            .filter(ReplicaInfo::isLeader)
                            .collect(Collectors.toUnmodifiableList())));
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
        return availableReplicaLeaders.getOrDefault(topic, List.of());
      }

      @Override
      public List<ReplicaInfo> availableReplicas(String topic) {
        return availableReplicas.getOrDefault(topic, List.of());
      }

      @Override
      public List<ReplicaInfo> replicas(String topic) {
        return replicas;
      }

      @Override
      public ClusterBean clusterBean() {
        return ClusterBean.of(Map.of());
      }
    };
  }

  /**
   * merge the beans into cluster information
   *
   * @param clusterInfo cluster information
   * @param beans extra beans
   * @return a new cluster information with extra beans
   */
  static ClusterInfo of(
      ClusterInfo clusterInfo, Map<Integer, ? extends Collection<HasBeanObject>> beans) {
    var clusterBean =
        ClusterBean.of(
            Stream.concat(
                    clusterInfo.clusterBean().all().entrySet().stream(), beans.entrySet().stream())
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        // Normally, the cluster info used to merge with beans has no beans. Hence,
                        // the cost of merge is acceptable
                        (l, r) ->
                            Stream.concat(l.stream(), r.stream())
                                .collect(Collectors.toUnmodifiableList()))));
    return new ClusterInfo() {

      @Override
      public List<NodeInfo> nodes() {
        return clusterInfo.nodes();
      }

      @Override
      public Set<String> dataDirectories(int brokerId) {
        return clusterInfo.dataDirectories(brokerId);
      }

      @Override
      public List<ReplicaInfo> availableReplicaLeaders(String topic) {
        return clusterInfo.availableReplicaLeaders(topic);
      }

      @Override
      public List<ReplicaInfo> availableReplicas(String topic) {
        return clusterInfo.availableReplicas(topic);
      }

      @Override
      public Set<String> topics() {
        return clusterInfo.topics();
      }

      @Override
      public List<ReplicaInfo> replicas(String topic) {
        return clusterInfo.replicas(topic);
      }

      @Override
      public ClusterBean clusterBean() {
        return clusterBean;
      }
    };
  }

  /**
   * find the node associated to specify node and port. Normally, the node + port should be unique
   * in cluster.
   *
   * @param host hostname
   * @param port client port
   * @return the node information. It throws NoSuchElementException if specify node and port is not
   *     associated to any node
   */
  default NodeInfo node(String host, int port) {
    return nodes().stream()
        .filter(n -> n.host().equals(host) && n.port() == port)
        .findAny()
        .orElseThrow(() -> new NoSuchElementException(host + ":" + port + " is nonexistent"));
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

  /** @return a {@link ClusterBean} used to get beanObject using a variety of different keys. */
  ClusterBean clusterBean();
}
