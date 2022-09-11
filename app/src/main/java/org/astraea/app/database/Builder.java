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
package org.astraea.app.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.common.Utils;

public class Builder {
  private String url;
  private String user;
  private String password;

  Builder() {}

  public Builder url(String url) {
    this.url = url;
    return this;
  }

  public Builder user(String user) {
    this.user = user;
    return this;
  }

  public Builder password(String password) {
    this.password = password;
    return this;
  }

  public DatabaseClient build() {
    return new DatabaseClient() {
      private final Connection connection =
          Utils.packException(
              () ->
                  DriverManager.getConnection(
                      Objects.requireNonNull(url, "url must be defined"),
                      Objects.requireNonNull(user, "user must be defined"),
                      Objects.requireNonNull(password, "password must be defined")));

      @Override
      public TableQuery query() {
        return new TableQuery() {
          private String catalog;
          private String schema;
          private String tableName;

          @Override
          public TableQuery catalog(String catalog) {
            this.catalog = catalog;
            return this;
          }

          @Override
          public TableQuery schema(String schema) {
            this.schema = schema;
            return this;
          }

          @Override
          public TableQuery tableName(String tableName) {
            this.tableName = tableName;
            return this;
          }

          @Override
          public Collection<TableInfo> run() {
            return Utils.packException(
                () -> {
                  var md = connection.getMetaData();
                  return catalogAndNames(md.getTables(catalog, schema, tableName, null)).stream()
                      .map(
                          e -> {
                            var catalog = e.getKey();
                            var tableName = e.getValue();
                            var pks =
                                primaryKeys(
                                    Utils.packException(
                                        () -> md.getPrimaryKeys(catalog, null, tableName)));
                            var columnNameAndTypes =
                                columnNameAndTypes(
                                    Utils.packException(
                                        () -> md.getColumns(catalog, null, tableName, null)));
                            return new TableInfo(
                                tableName,
                                columnNameAndTypes.entrySet().stream()
                                    .map(
                                        c ->
                                            new ColumnInfo(
                                                c.getKey(), c.getValue(), pks.contains(c.getKey())))
                                    .collect(Collectors.toUnmodifiableList()));
                          })
                      .collect(Collectors.toUnmodifiableList());
                });
          }
        };
      }

      @Override
      public TableCreator tableCreator() {
        return new TableCreator() {
          private String tableName;
          private final Map<String, String> nameAndType = new LinkedHashMap<>();
          private final Set<String> pks = new LinkedHashSet<>();

          @Override
          public TableCreator name(String name) {
            this.tableName = name;
            return this;
          }

          @Override
          public TableCreator column(String name, String type) {
            nameAndType.put(name, type);
            return this;
          }

          @Override
          public TableCreator primaryKey(String name, String type) {
            nameAndType.put(name, type);
            pks.add(name);
            return this;
          }

          @Override
          public void run() {
            if (pks.isEmpty()) throw new IllegalArgumentException("primary keys must be defined");
            var sql =
                "CREATE TABLE \""
                    + Objects.requireNonNull(tableName, "table name must be defined")
                    + "\" ( "
                    + nameAndType.entrySet().stream()
                        .map(e -> "\"" + e.getKey() + "\" " + e.getValue())
                        .collect(Collectors.joining(","))
                    + ", PRIMARY KEY ( "
                    + pks.stream().map(c -> "\"" + c + "\"").collect(Collectors.joining(","))
                    + " ))";
            execute(sql);
          }
        };
      }

      @Override
      public void deleteTable(String name) {
        execute("DROP TABLE \"" + name + "\"");
      }

      @Override
      public void close() {
        Utils.packException(connection::close);
      }

      private void execute(String sql) {
        try (var statement = connection.createStatement()) {
          statement.execute(sql);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static Map<String, String> columnNameAndTypes(ResultSet rs) {
    try (rs) {
      var result = new HashMap<String, String>();
      while (rs.next()) result.put(columnName(rs), columnType(rs));
      return Collections.unmodifiableMap(result);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static Set<String> primaryKeys(ResultSet rs) {
    try (rs) {
      var result = new HashSet<String>();
      while (rs.next()) result.add(columnName(rs));
      return Collections.unmodifiableSet(result);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static Collection<Map.Entry<String, String>> catalogAndNames(ResultSet rs) {
    try (rs) {
      var result = new ArrayList<Map.Entry<String, String>>();
      while (rs.next()) {
        if (isNormalTable(rs)) result.add(Map.entry(catalog(rs), tableName(rs)));
      }
      return Collections.unmodifiableList(result);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static String catalog(ResultSet rs) {
    return Utils.packException(() -> rs.getString("TABLE_CAT"));
  }

  private static String tableName(ResultSet rs) {
    return Utils.packException(() -> rs.getString("TABLE_NAME"));
  }

  private static String columnName(ResultSet rs) {
    return Utils.packException(() -> rs.getString("COLUMN_NAME"));
  }

  private static String columnType(ResultSet rs) {
    return Utils.packException(() -> rs.getString("TYPE_NAME"));
  }

  private static boolean isNormalTable(ResultSet rs) {
    return Utils.packException(
        () -> {
          var r = rs.getString("TABLE_TYPE");
          if (r == null) return true;
          return !Arrays.asList(r.split(" ")).contains("SYSTEM");
        });
  }
}
