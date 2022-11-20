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
