package org.astraea.app.service;

import org.astraea.app.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DatabaseTest {

  @Test
  void testConnectionInformation() {
    var user = Utils.randomString(10);
    var password = Utils.randomString(10);
    var databaseName = Utils.randomString(10);
    var port = Utils.availablePort();
    try (var database =
        Database.builder()
            .user(user)
            .password(password)
            .databaseName(databaseName)
            .port(port)
            .build()) {
      Assertions.assertEquals(Utils.hostname(), database.hostname());
      Assertions.assertEquals(port, database.port());
      Assertions.assertEquals(user, database.user());
      Assertions.assertEquals(password, database.password());
      Assertions.assertEquals(databaseName, database.databaseName());
    }
  }
}
