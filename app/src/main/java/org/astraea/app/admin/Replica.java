package org.astraea.app.admin;

import java.util.Objects;

public final class Replica {
  private final int broker;
  private final long lag;
  private final long size;
  private final boolean leader;
  private final boolean inSync;
  private final boolean isFuture;
  private final boolean offline;
  private final String path;

  Replica(
      int broker,
      long lag,
      long size,
      boolean leader,
      boolean inSync,
      boolean isFuture,
      boolean offline,
      String path) {
    this.broker = broker;
    this.lag = lag;
    this.size = size;
    this.leader = leader;
    this.inSync = inSync;
    this.isFuture = isFuture;
    this.offline = offline;
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
        && offline == replica.offline
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
        + ", offline="
        + offline
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

  /** @return true if the replica on the broker is offline. */
  public boolean isOffline() {
    return offline;
  }

  public String path() {
    return path;
  }
}
