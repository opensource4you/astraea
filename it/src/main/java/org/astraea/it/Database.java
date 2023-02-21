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

import static com.wix.mysql.EmbeddedMysql.anEmbeddedMysql;
import static com.wix.mysql.config.Charset.UTF8;
import static com.wix.mysql.config.MysqldConfig.aMysqldConfig;

import com.wix.mysql.distribution.Version;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public interface Database extends Closeable {
  static Database mysql() {
    return Database.builder().build();
  }

  static Builder builder() {
    return new Builder();
  }

  /**
   * @return hostname to connect
   */
  String hostname();

  /**
   * @return port to connect
   */
  int port();

  /**
   * @return database name of this mysql
   */
  String databaseName();

  /**
   * @return username to log in this mysql
   */
  String user();

  /**
   * @return password to log in this mysql
   */
  String password();

  /**
   * @return full JDBC url
   */
  String url();

  /**
   * @return create a new JDBC connection. Please close it manually
   */
  Connection createConnection();

  @Override
  void close();

  class Builder {
    private Builder() {}

    private String databaseName = "database-" + System.currentTimeMillis();
    private String user = "user";
    private String password = "password";
    private int port = 0;

    public Builder databaseName(String databaseName) {
      this.databaseName = databaseName;
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

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Database build() {
      var config =
          aMysqldConfig(Version.v5_7_latest)
              .withCharset(UTF8)
              .withUser(user, password)
              .withTimeZone(Calendar.getInstance().getTimeZone().getID())
              .withTimeout(2, TimeUnit.MINUTES)
              .withServerVariable("max_connect_errors", 666)
              .withTempDir(Utils.createTempDirectory("embedded_mysql").toAbsolutePath().toString())
              .withPort(Utils.resolvePort(port))
              // make mysql use " replace '
              // see https://stackoverflow.com/questions/13884854/mysql-double-quoted-table-names
              .withServerVariable("sql-mode", "ANSI_QUOTES")
              .build();
      var database = anEmbeddedMysql(config).addSchema(databaseName).start();
      return new Database() {
        @Override
        public void close() {
          database.stop();
        }

        @Override
        public String hostname() {
          return Utils.hostname();
        }

        @Override
        public int port() {
          return config.getPort();
        }

        @Override
        public String databaseName() {
          return databaseName;
        }

        @Override
        public String user() {
          return config.getUsername();
        }

        @Override
        public String password() {
          return config.getPassword();
        }

        @Override
        public String url() {
          return "jdbc:mysql://" + hostname() + ":" + port() + "/" + databaseName();
        }

        @Override
        public Connection createConnection() {
          try {
            return DriverManager.getConnection(url(), user(), password());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
    }
  }
}
