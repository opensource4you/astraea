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

import java.util.Objects;

public interface Replica extends ReplicaInfo {

  static Replica of(
      String topic,
      int partition,
      NodeInfo nodeInfo,
      long lag,
      long size,
      boolean leader,
      boolean inSync,
      boolean isFuture,
      boolean offline,
      boolean isPreferredLeader,
      String dataFolder) {
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

      /** @return true if the replica on the broker is offline. */
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

  /**
   * Whether this replica has been created by a AlterReplicaLogDirsRequest but not yet replaced the
   * current replica on the broker.
   *
   * @return true if this log is created by AlterReplicaLogDirsRequest and will replace the current
   *     log of the replica at some time in the future.
   */
  boolean isFuture();

  /** @return true if this is current log of replica. */
  default boolean isCurrent() {
    return !isFuture();
  }

  /** @return true if the replica is the preferred leader */
  boolean isPreferredLeader();

  long lag();

  long size();

  /**
   * @return that indicates the data folder path which stored this replica on a specific Kafka node.
   */
  String dataFolder();
}
