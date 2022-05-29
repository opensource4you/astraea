package org.astraea.database;

import java.util.Collection;
import java.util.Objects;

public class TableInfo {
  private final String name;
  private final Collection<ColumnInfo> columns;

  public TableInfo(String name, Collection<ColumnInfo> columns) {
    this.name = name;
    this.columns = columns;
  }

  public String name() {
    return name;
  }

  public Collection<ColumnInfo> columns() {
    return columns;
  }

  @Override
  public String toString() {
    return "TableInfo{" + "name='" + name + '\'' + ", columns=" + columns + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableInfo tableInfo = (TableInfo) o;
    return Objects.equals(name, tableInfo.name) && Objects.equals(columns, tableInfo.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, columns);
  }
}
