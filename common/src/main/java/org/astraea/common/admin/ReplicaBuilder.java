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

public class ReplicaBuilder {

  private String topic;
  private int partition;
  private NodeInfo nodeInfo;
  private long lag;
  private long size;
  private boolean leader;
  private boolean inSync;
  private boolean isFuture;
  private boolean offline;
  private boolean isPreferredLeader;
  private String dataFolder;

  public ReplicaBuilder replica(Replica replica) {
    this.topic = replica.topic();
    this.partition = replica.partition();
    this.nodeInfo = replica.nodeInfo();
    this.lag = replica.lag();
    this.size = replica.size();
    this.leader = replica.isLeader();
    this.inSync = replica.inSync();
    this.isFuture = replica.isFuture();
    this.offline = replica.isOffline();
    this.isPreferredLeader = replica.isPreferredLeader();
    this.dataFolder = replica.dataFolder();

    return this;
  }

  public ReplicaBuilder topic(String topic) {
    this.topic = topic;
    return this;
  }

  public ReplicaBuilder partition(int partition) {
    this.partition = partition;
    return this;
  }

  public ReplicaBuilder nodeInfo(NodeInfo nodeInfo) {
    this.nodeInfo = nodeInfo;
    return this;
  }

  public ReplicaBuilder lag(long lag) {
    this.lag = lag;
    return this;
  }

  public ReplicaBuilder size(long size) {
    this.size = size;
    return this;
  }

  public ReplicaBuilder leader(boolean leader) {
    this.leader = leader;
    return this;
  }

  public ReplicaBuilder inSync(boolean inSync) {
    this.inSync = inSync;
    return this;
  }

  public ReplicaBuilder isFuture(boolean isFuture) {
    this.isFuture = isFuture;
    return this;
  }

  public ReplicaBuilder offline(boolean offline) {
    this.offline = offline;
    return this;
  }

  public ReplicaBuilder isPreferredLeader(boolean isPreferredLeader) {
    this.isPreferredLeader = isPreferredLeader;
    return this;
  }

  public ReplicaBuilder dataFolder(String dataFolder) {
    this.dataFolder = dataFolder;
    return this;
  }

  public Replica build() {
    // todo is optional or necessary
    Objects.requireNonNull(this.nodeInfo);
    Objects.requireNonNull(this.topic);
    Objects.requireNonNull(this.dataFolder);

    return new Replica() {
      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var replica = (Replica) o;
        return partition == replica.partition()
            && Objects.equals(nodeInfo, replica.nodeInfo())
            && lag == replica.lag()
            && size == replica.size()
            && leader == replica.isLeader()
            && inSync == replica.inSync()
            && isFuture == replica.isFuture()
            && isPreferredLeader == replica.isPreferredLeader()
            && offline == replica.isOffline()
            && Objects.equals(topic, replica.topic())
            && Objects.equals(dataFolder, replica.dataFolder());
      }

      @Override
      public int hashCode() {
        return Objects.hash(
            topic,
            partition,
            nodeInfo,
            lag,
            size,
            leader,
            inSync,
            isFuture,
            isPreferredLeader,
            offline,
            dataFolder);
      }

      @Override
      public String toString() {
        return "Replica{"
            + "topic='"
            + topic
            + '\''
            + ", partition="
            + partition
            + ", broker="
            + nodeInfo
            + ", lag="
            + lag
            + ", size="
            + size
            + ", leader="
            + leader
            + ", inSync="
            + inSync
            + ", isFuture="
            + isFuture
            + ", isPreferredLeader="
            + isPreferredLeader
            + ", offline="
            + offline
            + ", path='"
            + dataFolder
            + '\''
            + '}';
      }

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
      public long lag() {
        return lag;
      }

      @Override
      public long size() {
        return size;
      }

      @Override
      public boolean isLeader() {
        return leader;
      }

      @Override
      public boolean inSync() {
        return inSync;
      }

      @Override
      public boolean isFuture() {
        return isFuture;
      }

      @Override
      public boolean isPreferredLeader() {
        return isPreferredLeader;
      }

      @Override
      public boolean isOffline() {
        return offline;
      }

      @Override
      public String dataFolder() {
        return dataFolder;
      }
    };
  }
}
