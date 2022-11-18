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

import org.astraea.it.Database;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DatabaseClientTest {

  private static String randomString(int size) {
    return java.util.UUID.randomUUID().toString().replaceAll("-", "").substring(0, size);
  }

  @Test
  void testCreate() {
    try (var database = Database.builder().build();
        var client =
            DatabaseClient.builder()
                .user(database.user())
                .password(database.password())
                .url(database.url())
                .build()) {
      var tableCount = client.query().run().size();
      var tableName = randomString(10);
      var c0 = new ColumnInfo(randomString(5), "INT", true);
      var c1 = new ColumnInfo(randomString(5), "INT", false);
      client
          .tableCreator()
          .name(tableName)
          .primaryKey(c0.name(), c0.type())
          .column(c1.name(), c1.type())
          .run();

      Assertions.assertEquals(tableCount + 1, client.query().run().size());
      Assertions.assertEquals(1, client.query().tableName(tableName).run().size());
      var table = client.query().tableName(tableName).run().iterator().next();
      Assertions.assertEquals(2, table.columns().size());
      Assertions.assertEquals(
          c0, table.columns().stream().filter(c -> c.name().equals(c0.name())).findFirst().get());
      Assertions.assertEquals(
          c1, table.columns().stream().filter(c -> c.name().equals(c1.name())).findFirst().get());
    }
  }

  @Test
  void testDelete() {
    try (var database = Database.builder().build();
        var client =
            DatabaseClient.builder()
                .user(database.user())
                .password(database.password())
                .url(database.url())
                .build()) {
      var tableName = randomString(10);
      var tableCount = client.query().run().size();
      client.tableCreator().name(tableName).primaryKey(randomString(5), "INT").run();
      Assertions.assertEquals(tableCount + 1, client.query().run().size());
      client.deleteTable(tableName);
      Assertions.assertEquals(tableCount, client.query().run().size());
    }
  }

  @Test
  void testNoPrimaryKeyOrTableName() {
    try (var database = Database.builder().build();
        var client =
            DatabaseClient.builder()
                .user(database.user())
                .password(database.password())
                .url(database.url())
                .build()) {
      Assertions.assertThrows(
          NullPointerException.class,
          () -> client.tableCreator().primaryKey(randomString(5), "INT").run());
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> client.tableCreator().name("a").column(randomString(5), "INT").run());
    }
  }
}
