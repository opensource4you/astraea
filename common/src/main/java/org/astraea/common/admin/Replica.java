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

public interface Replica extends ReplicaInfo {

  static ReplicaBuilder builder() {
    return new ReplicaBuilder();
  }

  static ReplicaBuilder builder(Replica replica) {
    return Replica.builder().replica(replica);
  }

  /**
   * Whether this replica has been created by a AlterReplicaLogDirsRequest but not yet replaced the
   * current replica on the broker.
   *
   * @return true if this log is created by AlterReplicaLogDirsRequest and will replace the current
   *     log of the replica at some time in the future.
   */
  boolean isFuture();

  /**
   * @return true if this is current log of replica.
   */
  default boolean isCurrent() {
    return !isFuture();
  }

  /**
   * @return true if the replica is the preferred leader
   */
  boolean isPreferredLeader();

  /**
   * @return (LEO - high watermark) if it is the current log, * (LEO) if it is the future log, *
   *     (-1) if the host of replica is offline
   */
  long lag();

  /**
   * @return The size of all log segments in this replica in bytes. It returns -1 if the host of
   *     replica is offline
   */
  long size();

  /**
   * @return that indicates the data folder path which stored this replica on a specific Kafka node.
   *     It returns null if the host of replica is offline
   */
  String path();

  /**
   * @return true if this replica belongs to internal topic
   */
  boolean internal();
}
