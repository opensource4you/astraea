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

import java.util.Objects;

/**
 * @param topic topic name
 * @param partition partition id
 * @param broker information of the node hosts this replica
 * @param isLeader true if this replica is a leader replica
 * @param isSync true if this replica is synced
 * @param isOffline true if this replica is offline
 * @param isAdding true if this replica is adding and syncing data
 * @param isRemoving true if this replica will be deleted in the future.
 * @param isFuture true if this log is created by AlterReplicaLogDirsRequest and will replace the
 *     current log of the replica at some time in the future.
 * @param isPreferredLeader true if the replica is the preferred leader
 * @param lag (LEO - high watermark) if it is the current log, * (LEO) if it is the future log, (-1)
 *     if the host of replica is offline
 * @param size The size of all log segments in this replica in bytes. It returns -1 if the host of
 *     replica is offline
 * @param path that indicates the data folder path which stored this replica on a specific Kafka
 *     node. It returns null if the host of replica is offline
 * @param isInternal true if this replica belongs to internal topic
 */
public record Replica(
    String topic,
    int partition,
    int brokerId,
    boolean isLeader,
    boolean isSync,
    boolean isOffline,
    boolean isAdding,
    boolean isRemoving,
    boolean isFuture,
    boolean isPreferredLeader,
    long lag,
    long size,
    String path,
    boolean isInternal) {

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Replica replica) {
    return Replica.builder().replica(replica);
  }

  /**
   * a helper to build TopicPartitionReplica quickly
   *
   * @return TopicPartitionReplica
   */
  public TopicPartitionReplica topicPartitionReplica() {
    return TopicPartitionReplica.of(topic(), partition(), brokerId());
  }

  /**
   * a helper to build TopicPartition quickly
   *
   * @return TopicPartition
   */
  public TopicPartition topicPartition() {
    return TopicPartition.of(topic(), partition());
  }

  /**
   * @return true if this replica is a follower replica
   */
  public boolean isFollower() {
    return !isLeader();
  }

  /**
   * @return true if this replica is online
   */
  public boolean isOnline() {
    return !isOffline();
  }

  /**
   * @return true if this is current log of replica.
   */
  public boolean isCurrent() {
    return !isFuture();
  }

  public static class Builder {

    private String topic;
    private int partition;
    private int brokerId;
    private long lag;
    private long size;

    private boolean isAdding;

    private boolean isRemoving;
    private boolean isInternal;
    private boolean isLeader;
    private boolean isSync;
    private boolean isFuture;
    private boolean isOffline;
    private boolean isPreferredLeader;
    private String path;

    private Builder() {}

    public Builder replica(Replica replica) {
      this.topic = replica.topic();
      this.partition = replica.partition();
      this.brokerId = replica.brokerId();
      this.lag = replica.lag();
      this.size = replica.size();
      this.isAdding = replica.isAdding;
      this.isRemoving = replica.isRemoving;
      this.isInternal = replica.isInternal;
      this.isLeader = replica.isLeader();
      this.isSync = replica.isSync();
      this.isFuture = replica.isFuture();
      this.isOffline = replica.isOffline();
      this.isPreferredLeader = replica.isPreferredLeader();
      this.path = replica.path();
      return this;
    }

    public Builder topic(String topic) {
      this.topic = topic;
      return this;
    }

    public Builder partition(int partition) {
      this.partition = partition;
      return this;
    }

    public Builder brokerId(int brokerId) {
      this.brokerId = brokerId;
      return this;
    }

    public Builder lag(long lag) {
      this.lag = lag;
      return this;
    }

    public Builder size(long size) {
      this.size = size;
      return this;
    }

    public Builder isAdding(boolean isAdding) {
      this.isAdding = isAdding;
      return this;
    }

    public Builder isRemoving(boolean isRemoving) {
      this.isRemoving = isRemoving;
      return this;
    }

    public Builder isInternal(boolean isInternal) {
      this.isInternal = isInternal;
      return this;
    }

    public Builder isLeader(boolean leader) {
      this.isLeader = leader;
      return this;
    }

    public Builder isSync(boolean isSync) {
      this.isSync = isSync;
      return this;
    }

    public Builder isFuture(boolean isFuture) {
      this.isFuture = isFuture;
      return this;
    }

    public Builder isOffline(boolean offline) {
      this.isOffline = offline;
      return this;
    }

    public Builder isPreferredLeader(boolean isPreferredLeader) {
      this.isPreferredLeader = isPreferredLeader;
      return this;
    }

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    /**
     * a helper used to set all flags for a replica leader.
     *
     * @return a replica leader
     */
    public Replica buildLeader() {
      return new Replica(
          Objects.requireNonNull(topic),
          partition,
          brokerId,
          true,
          true,
          false,
          false,
          false,
          false,
          true,
          0,
          size,
          Objects.requireNonNull(path),
          isInternal);
    }

    /**
     * a helper used to set all flags for a in-sync replica follower.
     *
     * @return a replica leader
     */
    public Replica buildInSyncFollower() {
      return new Replica(
          Objects.requireNonNull(topic),
          partition,
          brokerId,
          false,
          true,
          false,
          false,
          false,
          false,
          false,
          0,
          size,
          Objects.requireNonNull(path),
          isInternal);
    }

    public Replica build() {
      return new Replica(
          Objects.requireNonNull(topic),
          partition,
          brokerId,
          isLeader,
          isSync,
          isOffline,
          isAdding,
          isRemoving,
          isFuture,
          isPreferredLeader,
          lag,
          size,
          path,
          isInternal);
    }
  }
}
