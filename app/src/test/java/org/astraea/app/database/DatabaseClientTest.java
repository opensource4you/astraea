package org.astraea.app.database;

import org.astraea.app.common.Utils;
import org.astraea.app.service.Database;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DatabaseClientTest {
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
      var tableName = Utils.randomString(10);
      var c0 = new ColumnInfo(Utils.randomString(5), "INT", true);
      var c1 = new ColumnInfo(Utils.randomString(5), "INT", false);
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
      var tableName = Utils.randomString(10);
      var tableCount = client.query().run().size();
      client.tableCreator().name(tableName).primaryKey(Utils.randomString(5), "INT").run();
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
          () -> client.tableCreator().primaryKey(Utils.randomString(5), "INT").run());
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> client.tableCreator().name("a").column(Utils.randomString(5), "INT").run());
    }
  }
}
