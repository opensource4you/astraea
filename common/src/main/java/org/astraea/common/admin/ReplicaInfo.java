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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public interface ReplicaInfo {

  static List<ReplicaInfo> of(org.apache.kafka.common.PartitionInfo pf) {
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
        .collect(Collectors.toUnmodifiableList());
  }

  static ReplicaInfo of(
      String topic,
      int partition,
      NodeInfo nodeInfo,
      boolean isLeader,
      boolean isSynced,
      boolean isOffline) {
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
      public boolean isSync() {
        return isSynced;
      }

      @Override
      public boolean isOffline() {
        return isOffline;
      }

      @Override
      public boolean isAdding() {
        return false;
      }

      @Override
      public boolean isRemoving() {
        return false;
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
            + (isLeader() ? " leader" : "")
            + (isSync() ? ":synced" : "")
            + (isOffline() ? ":offline" : "")
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
              && this.isSync() == that.isSync()
              && this.isOffline() == that.isOffline();
        }
        return false;
      }

      @Override
      public int hashCode() {
        return Objects.hash(topic, partition, nodeInfo, isLeader, isSynced, isOffline);
      }
    };
  }

  /**
   * a helper to build TopicPartitionReplica quickly
   *
   * @return TopicPartitionReplica
   */
  default TopicPartitionReplica topicPartitionReplica() {
    return TopicPartitionReplica.of(topic(), partition(), nodeInfo().id());
  }

  /**
   * a helper to build TopicPartition quickly
   *
   * @return TopicPartition
   */
  default TopicPartition topicPartition() {
    return TopicPartition.of(topic(), partition());
  }

  /**
   * @return topic name
   */
  String topic();

  /**
   * @return partition id
   */
  int partition();

  /**
   * @return information of the node hosts this replica
   */
  NodeInfo nodeInfo();

  /**
   * @return true if this replica is a leader replica
   */
  boolean isLeader();

  /**
   * @return true if this replica is a follower replica
   */
  default boolean isFollower() {
    return !isLeader();
  }

  /**
   * @return true if this replica is synced
   */
  boolean isSync();

  /**
   * @return true if this replica is offline
   */
  boolean isOffline();

  /**
   * @return true if this replica is online
   */
  default boolean isOnline() {
    return !isOffline();
  }

  /**
   * @return true if this replica is adding and syncing data
   */
  boolean isAdding();

  /**
   * @return true if this replica will be deleted in the future.
   */
  boolean isRemoving();
}
