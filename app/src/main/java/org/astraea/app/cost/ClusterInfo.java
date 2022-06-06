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
package org.astraea.app.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.app.metrics.HasBeanObject;

public interface ClusterInfo {

  static ClusterInfo of(org.apache.kafka.common.Cluster cluster) {
    return new ClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return cluster.nodes().stream().map(NodeInfo::of).collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Set<String> dataDirectories(int brokerId) {
        // org.apache.kafka.common.Cluster doesn't have such information.
        throw new UnsupportedOperationException("This information is not available");
      }

      public Set<String> topics() {
        return cluster.topics();
      }

      @Override
      public List<ReplicaInfo> availableReplicaLeaders(String topic) {
        return cluster.availablePartitionsForTopic(topic).stream()
            .map(ReplicaInfo::of)
            .map(
                replicas ->
                    replicas.stream().filter(ReplicaInfo::isLeader).findFirst().orElseThrow())
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public List<ReplicaInfo> availableReplicas(String topic) {
        return cluster.availablePartitionsForTopic(topic).stream()
            .map(ReplicaInfo::of)
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public List<ReplicaInfo> replicas(String topic) {
        return cluster.partitionsForTopic(topic).stream()
            .map(ReplicaInfo::of)
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Collection<HasBeanObject> beans(int brokerId) {
        return List.of();
      }

      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return Map.of();
      }
    };
  }

  /**
   * merge the beans into cluster information
   *
   * @param cluster cluster information
   * @param beans extra beans
   * @return a new cluster information with extra beans
   */
  static ClusterInfo of(ClusterInfo cluster, Map<Integer, Collection<HasBeanObject>> beans) {
    var all = new HashMap<Integer, List<HasBeanObject>>();
    cluster
        .allBeans()
        .forEach((key, value) -> all.computeIfAbsent(key, k -> new ArrayList<>()).addAll(value));
    beans.forEach((key, value) -> all.computeIfAbsent(key, k -> new ArrayList<>()).addAll(value));
    return new ClusterInfo() {

      @Override
      public List<NodeInfo> nodes() {
        return cluster.nodes();
      }

      @Override
      public Set<String> dataDirectories(int brokerId) {
        return cluster.dataDirectories(brokerId);
      }

      @Override
      public List<ReplicaInfo> availableReplicaLeaders(String topic) {
        return cluster.availableReplicaLeaders(topic);
      }

      @Override
      public List<ReplicaInfo> availableReplicas(String topic) {
        return cluster.availableReplicas(topic);
      }

      @Override
      public Set<String> topics() {
        return cluster.topics();
      }

      @Override
      public List<ReplicaInfo> replicas(String topic) {
        return cluster.replicas(topic);
      }

      @Override
      public Collection<HasBeanObject> beans(int brokerId) {
        return all.getOrDefault(brokerId, List.of());
      }

      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return Collections.unmodifiableMap(all);
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
        .orElseThrow(() -> new IllegalArgumentException(id + " is nonexistent"));
  }

  /** @return The known set of nodes */
  List<NodeInfo> nodes();

  /** @return return the data directories on specific broker */
  Set<String> dataDirectories(int brokerId);
  // TODO: provide a ClusterInfo implementation with this info

  /**
   * Get the list of replica leader information of each available partition for the given topic
   *
   * @param topic The Topic name
   * @return A list of {@link ReplicaInfo}
   */
  List<ReplicaInfo> availableReplicaLeaders(String topic);

  /**
   * Get the list of replica information of each available partition/replica pair for the given
   * topic
   *
   * @param topic The topic name
   * @return A list of {@link ReplicaInfo}
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
   * @return A list of {@link ReplicaInfo}
   */
  List<ReplicaInfo> replicas(String topic);

  /**
   * @param brokerId broker id
   * @return return the metrics of broker. It returns empty collection if the broker id is
   *     nonexistent
   */
  Collection<HasBeanObject> beans(int brokerId);

  /** @return all beans of all brokers */
  Map<Integer, Collection<HasBeanObject>> allBeans();
}
