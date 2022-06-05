package org.astraea.app.admin;

public final class Offset {
  private final long earliest;
  private final long latest;

  Offset(long earliest, long latest) {
    this.earliest = earliest;
    this.latest = latest;
  }

  @Override
  public String toString() {
    return "Offset{" + "earliest=" + earliest + ", latest=" + latest + '}';
  }

  public long earliest() {
    return earliest;
  }

  public long latest() {
    return latest;
  }
}
