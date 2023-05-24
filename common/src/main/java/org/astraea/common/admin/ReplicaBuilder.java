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
  private Broker broker;
  private long lag;
  private long size;

  private boolean isAdding;

  private boolean isRemoving;
  private boolean internal;
  private boolean isLeader;
  private boolean isSync;
  private boolean isFuture;
  private boolean isOffline;
  private boolean isPreferredLeader;
  private String path;

  ReplicaBuilder replica(Replica replica) {
    this.topic = replica.topic();
    this.partition = replica.partition();
    this.broker = replica.broker();
    this.lag = replica.lag();
    this.size = replica.size();
    this.isLeader = replica.isLeader();
    this.isSync = replica.isSync();
    this.isFuture = replica.isFuture();
    this.isOffline = replica.isOffline();
    this.isPreferredLeader = replica.isPreferredLeader();
    this.path = replica.path();

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

  public ReplicaBuilder broker(Broker broker) {
    this.broker = broker;
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

  public ReplicaBuilder isAdding(boolean isAdding) {
    this.isAdding = isAdding;
    return this;
  }

  public ReplicaBuilder isRemoving(boolean isRemoving) {
    this.isRemoving = isRemoving;
    return this;
  }

  public ReplicaBuilder internal(boolean internal) {
    this.internal = internal;
    return this;
  }

  public ReplicaBuilder isLeader(boolean leader) {
    this.isLeader = leader;
    return this;
  }

  public ReplicaBuilder isSync(boolean isSync) {
    this.isSync = isSync;
    return this;
  }

  public ReplicaBuilder isFuture(boolean isFuture) {
    this.isFuture = isFuture;
    return this;
  }

  public ReplicaBuilder isOffline(boolean offline) {
    this.isOffline = offline;
    return this;
  }

  public ReplicaBuilder isPreferredLeader(boolean isPreferredLeader) {
    this.isPreferredLeader = isPreferredLeader;
    return this;
  }

  public ReplicaBuilder path(String path) {
    this.path = path;
    return this;
  }

  /**
   * a helper used to set all flags for a replica leader.
   *
   * @return a replica leader
   */
  public Replica buildLeader() {
    Objects.requireNonNull(path);
    return new ReplicaImpl(
        this.isLeader(true)
            .isPreferredLeader(true)
            .isSync(true)
            .isFuture(false)
            .isOffline(false)
            .isRemoving(false)
            .isAdding(false)
            .lag(0));
  }

  /**
   * a helper used to set all flags for a in-sync replica follower.
   *
   * @return a replica leader
   */
  public Replica buildInSyncFollower() {
    Objects.requireNonNull(path);
    return new ReplicaImpl(
        this.isLeader(false)
            .isPreferredLeader(false)
            .isSync(true)
            .isFuture(false)
            .isOffline(false)
            .isRemoving(false)
            .isAdding(false)
            .lag(0));
  }

  public Replica build() {
    return new ReplicaImpl(this);
  }

  private static class ReplicaImpl implements Replica {
    private final String topic;
    private final int partition;
    private final Broker broker;
    private final long lag;
    private final long size;

    private final boolean internal;
    private final boolean isLeader;

    private final boolean isAdding;

    private final boolean isRemoving;
    private final boolean isSync;
    private final boolean isFuture;
    private final boolean isOffline;
    private final boolean isPreferredLeader;
    private final String path;

    private ReplicaImpl(ReplicaBuilder builder) {
      this.topic = Objects.requireNonNull(builder.topic);
      this.partition = builder.partition;
      this.broker = Objects.requireNonNull(builder.broker);
      this.isAdding = builder.isAdding;
      this.isRemoving = builder.isRemoving;
      this.lag = builder.lag;
      this.size = builder.size;
      this.internal = builder.internal;
      this.isLeader = builder.isLeader;
      this.isSync = builder.isSync;
      this.isFuture = builder.isFuture;
      this.isOffline = builder.isOffline;
      this.isPreferredLeader = builder.isPreferredLeader;
      this.path = builder.path;
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
    public long lag() {
      return lag;
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public String path() {
      return path;
    }

    @Override
    public boolean internal() {
      return internal;
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
    public Broker broker() {
      return broker;
    }

    @Override
    public boolean isLeader() {
      return isLeader;
    }

    @Override
    public boolean isSync() {
      return isSync;
    }

    @Override
    public boolean isOffline() {
      return isOffline;
    }

    @Override
    public boolean isAdding() {
      return isAdding;
    }

    @Override
    public boolean isRemoving() {
      return isRemoving;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ReplicaImpl replica = (ReplicaImpl) o;
      return partition == replica.partition
          && lag == replica.lag
          && size == replica.size
          && internal == replica.internal
          && isLeader == replica.isLeader
          && isAdding == replica.isAdding
          && isRemoving == replica.isRemoving
          && isSync == replica.isSync
          && isFuture == replica.isFuture
          && isOffline == replica.isOffline
          && isPreferredLeader == replica.isPreferredLeader
          && topic.equals(replica.topic)
          && broker.id() == replica.broker.id()
          && broker.host().equals(replica.broker.host())
          && broker.port() == replica.broker.port()
          && Objects.equals(path, replica.path);
    }

    @Override
    public String toString() {
      return "Replica{"
          + "topic='"
          + topic()
          + '\''
          + ", partition="
          + partition()
          + ", broker="
          + broker()
          + ", path='"
          + path()
          + '\''
          + ", "
          + (isOffline() ? "offline" : isLeader() ? "leader" : "follower")
          + '}';
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          topic,
          partition,
          broker,
          lag,
          size,
          internal,
          isLeader,
          isAdding,
          isRemoving,
          isSync,
          isFuture,
          isOffline,
          isPreferredLeader,
          path);
    }
  }
}
