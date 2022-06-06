package org.astraea.app.database;

import java.util.Objects;

public class ColumnInfo {

  private final String name;
  private final String type;
  private final boolean pk;

  public ColumnInfo(String name, String type, boolean pk) {
    this.name = name;
    this.type = type;
    this.pk = pk;
  }

  public String name() {
    return name;
  }

  public String type() {
    return type;
  }

  public boolean pk() {
    return pk;
  }

  @Override
  public String toString() {
    return "ColumnInfo{" + "name='" + name + '\'' + ", type='" + type + '\'' + ", pk=" + pk + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnInfo that = (ColumnInfo) o;
    return pk == that.pk && Objects.equals(name, that.name) && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, pk);
  }
}
