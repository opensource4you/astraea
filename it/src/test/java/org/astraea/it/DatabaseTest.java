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
package org.astraea.it;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DatabaseTest {

  @Test
  void testConnectionInformation() {
    var user = "user-" + System.currentTimeMillis();
    var password = "password-" + System.currentTimeMillis();
    var databaseName = "database-" + System.currentTimeMillis();
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
