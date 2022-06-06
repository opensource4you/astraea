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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public interface ReplicaInfo {

  static Set<ReplicaInfo> of(org.apache.kafka.common.PartitionInfo pf) {
    final var replicas = List.of(pf.replicas());
    final var leaderReplica = List.of(pf.leader());
    final var inSyncReplicas = List.of(pf.inSyncReplicas());
    final var offlineReplicas = List.of(pf.offlineReplicas());

    return replicas.stream()
        .map(
            node ->
                of(
                    pf.topic(),
                    pf.partition(),
                    NodeInfo.of(node),
                    leaderReplica.contains(node),
                    inSyncReplicas.contains(node),
                    offlineReplicas.contains(node)))
        .collect(Collectors.toUnmodifiableSet());
  }

  static ReplicaInfo of(
      String topic,
      int partition,
      NodeInfo nodeInfo,
      boolean isLeader,
      boolean isSynced,
      boolean isOfflineReplica) {
    return of(topic, partition, nodeInfo, isLeader, isSynced, isOfflineReplica, null);
  }

  static ReplicaInfo of(
      String topic,
      int partition,
      NodeInfo nodeInfo,
      boolean isLeader,
      boolean isSynced,
      boolean isOfflineReplica,
      String dataFolder) {
    return new ReplicaInfo() {
      @Override
      public String topic() {
        return topic;
      }

      @Override
      public int partition() {
        return partition;
      }

      @Override
      public NodeInfo nodeInfo() {
        return nodeInfo;
      }

      @Override
      public boolean isLeader() {
        return isLeader;
      }

      @Override
      public boolean inSync() {
        return isSynced;
      }

      @Override
      public boolean isOfflineReplica() {
        return isOfflineReplica;
      }

      @Override
      public Optional<String> dataFolder() {
        return Optional.ofNullable(dataFolder);
      }

      @Override
      public String toString() {
        return "ReplicaInfo {"
            + "topic=\""
            + topic
            + "\" partition="
            + partition
            + " replicaAtBroker="
            + nodeInfo.id()
            + " dataFolder="
            + (dataFolder().map(x -> "\"" + x + "\"").orElse("unknown"))
            + (isLeader() ? " leader" : "")
            + (isFollower() ? " follower" : "")
            + (inSync() ? ":synced" : "")
            + (isOfflineReplica() ? ":offline" : "")
            + "}";
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof ReplicaInfo) {
          var that = ((ReplicaInfo) obj);

          return this.topic().equals(that.topic())
              && this.partition() == that.partition()
              && this.nodeInfo().equals(that.nodeInfo())
              && this.isLeader() == that.isLeader()
              && this.isFollower() == that.isFollower()
              && this.inSync() == that.inSync()
              && this.isOfflineReplica() == that.isOfflineReplica()
              && this.dataFolder().equals(that.dataFolder());
        }
        return false;
      }
    };
  }

  /** @return topic name */
  String topic();

  /** @return partition id */
  int partition();

  /** @return information of the node hosts this replica */
  NodeInfo nodeInfo();

  /** @return true if this replica is a leader replica */
  boolean isLeader();

  /** @return true if this replica is a follower replica */
  default boolean isFollower() {
    return !isLeader();
  }

  /** @return true if this replica is synced */
  boolean inSync();

  /** @return true if this replica is offline */
  boolean isOfflineReplica();

  /**
   * The path to the data folder which hosts this replica. Since this information is not very openly
   * available. An application might find it hard to retrieve this information(for example, the
   * producer client might need to initialize an AdminClient to access this information). To provide
   * this information or not is totally up to the caller.
   *
   * @return a {@link Optional<String>} that indicates the data folder path which stored this
   *     replica on a specific Kafka node.
   */
  Optional<String> dataFolder();
}
