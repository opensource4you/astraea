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

public final class Replica {
  private final int broker;
  private final long lag;
  private final long size;
  private final boolean leader;
  private final boolean inSync;
  private final boolean isFuture;
  private final String path;

  Replica(
      int broker,
      long lag,
      long size,
      boolean leader,
      boolean inSync,
      boolean isFuture,
      String path) {
    this.broker = broker;
    this.lag = lag;
    this.size = size;
    this.leader = leader;
    this.inSync = inSync;
    this.isFuture = isFuture;
    this.path = path;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Replica replica = (Replica) o;
    return broker == replica.broker
        && lag == replica.lag
        && size == replica.size
        && leader == replica.leader
        && inSync == replica.inSync
        && isFuture == replica.isFuture
        && path.equals(replica.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(broker, lag, size, leader, inSync, isFuture, path);
  }

  @Override
  public String toString() {
    return "Replica{"
        + "broker="
        + broker
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
        + ", path='"
        + path
        + '\''
        + '}';
  }

  public int broker() {
    return broker;
  }

  public long lag() {
    return lag;
  }

  public long size() {
    return size;
  }

  public boolean leader() {
    return leader;
  }

  public boolean inSync() {
    return inSync;
  }

  /**
   * Whether this replica has been created by a AlterReplicaLogDirsRequest but not yet replaced the
   * current replica on the broker.
   *
   * @return true if this log is created by AlterReplicaLogDirsRequest and will replace the current
   *     log of the replica at some time in the future.
   */
  public boolean isFuture() {
    return isFuture;
  }

  /** @return true if this is current log of replica. */
  public boolean isCurrent() {
    return !isFuture;
  }

  public String path() {
    return path;
  }
}
